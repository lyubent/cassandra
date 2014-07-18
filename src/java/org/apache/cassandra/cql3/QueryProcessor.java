/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.primitives.Ints;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EntryWeigher;
import org.antlr.runtime.*;
import org.github.jamm.MemoryMeter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.recording.QueryRecorder;
import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ThriftClientState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.SemanticVersion;

public class QueryProcessor implements QueryHandler
{
    public static final SemanticVersion CQL_VERSION = new SemanticVersion("3.1.5");

    public static final QueryProcessor instance = new QueryProcessor();

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
    private static final MemoryMeter meter = new MemoryMeter().withGuessing(MemoryMeter.Guess.FALLBACK_BEST);
    private static final long MAX_CACHE_PREPARED_MEMORY = Runtime.getRuntime().maxMemory() / 256;
    private static volatile AtomicInteger querylogCounter = new AtomicInteger(0);

    private static EntryWeigher<MD5Digest, CQLStatement> cqlMemoryUsageWeigher = new EntryWeigher<MD5Digest, CQLStatement>()
    {
        @Override
        public int weightOf(MD5Digest key, CQLStatement value)
        {
            return Ints.checkedCast(measure(key) + measure(value));
        }
    };

    private static EntryWeigher<Integer, CQLStatement> thriftMemoryUsageWeigher = new EntryWeigher<Integer, CQLStatement>()
    {
        @Override
        public int weightOf(Integer key, CQLStatement value)
        {
            return Ints.checkedCast(measure(key) + measure(value));
        }
    };

    private static final ConcurrentLinkedHashMap<MD5Digest, CQLStatement> preparedStatements;
    private static final ConcurrentLinkedHashMap<Integer, CQLStatement> thriftPreparedStatements;

    static
    {
        preparedStatements = new ConcurrentLinkedHashMap.Builder<MD5Digest, CQLStatement>()
                             .maximumWeightedCapacity(MAX_CACHE_PREPARED_MEMORY)
                             .weigher(cqlMemoryUsageWeigher)
                             .build();
        thriftPreparedStatements = new ConcurrentLinkedHashMap.Builder<Integer, CQLStatement>()
                                   .maximumWeightedCapacity(MAX_CACHE_PREPARED_MEMORY)
                                   .weigher(thriftMemoryUsageWeigher)
                                   .build();
    }

    private QueryProcessor()
    {
    }

    public CQLStatement getPrepared(MD5Digest id)
    {
        return preparedStatements.get(id);
    }

    public CQLStatement getPreparedForThrift(Integer id)
    {
        return thriftPreparedStatements.get(id);
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public static void validateCellNames(Iterable<CellName> cellNames, CellNameType type) throws InvalidRequestException
    {
        for (CellName name : cellNames)
            validateCellName(name, type);
    }

    public static void validateCellName(CellName name, CellNameType type) throws InvalidRequestException
    {
        validateComposite(name, type);
        if (name.isEmpty())
            throw new InvalidRequestException("Invalid empty value for clustering column of COMPACT TABLE");
    }

    public static void validateComposite(Composite name, CType type) throws InvalidRequestException
    {
        long serializedSize = type.serializer().serializedSize(name, TypeSizes.NATIVE);
        if (serializedSize > Cell.MAX_NAME_LENGTH)
            throw new InvalidRequestException(String.format("The sum of all clustering columns is too long (%s > %s)",
                                                            serializedSize,
                                                            Cell.MAX_NAME_LENGTH));
    }

    public static ResultMessage processStatement(CQLStatement statement,
                                                  QueryState queryState,
                                                  QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        logger.trace("Process {} @CL.{}", statement, options.getConsistency());
        ClientState clientState = queryState.getClientState();
        statement.checkAccess(clientState);
        statement.validate(clientState);

        ResultMessage result = statement.execute(queryState, options);
        return result == null ? new ResultMessage.Void() : result;
    }

    public static ResultMessage process(String queryString, ConsistencyLevel cl, QueryState queryState)
    throws RequestExecutionException, RequestValidationException
    {
        return instance.process(queryString, queryState, new QueryOptions(cl, Collections.<ByteBuffer>emptyList()));
    }

    public ResultMessage process(String queryString, QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        CQLStatement prepared = getStatement(queryString, queryState.getClientState()).statement;

        if (prepared.getBoundTerms() != options.getValues().size())
            throw new InvalidRequestException("Invalid amount of bind variables");

        maybeLogQuery(0, null, queryString, queryState.getClientState(), prepared, null);
        return processStatement(prepared, queryState, options);
    }

    public static CQLStatement parseStatement(String queryStr, QueryState queryState) throws RequestValidationException
    {
        return getStatement(queryStr, queryState.getClientState()).statement;
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        try
        {
            ResultMessage result = instance.process(query, QueryState.forInternalCalls(), new QueryOptions(cl, Collections.<ByteBuffer>emptyList()));
            if (result instanceof ResultMessage.Rows)
                return UntypedResultSet.create(((ResultMessage.Rows)result).result);
            else
                return null;
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static UntypedResultSet processInternal(String query)
    {
        try
        {
            ClientState state = ClientState.forInternalCalls();
            QueryState qState = new QueryState(state);
            state.setKeyspace(Keyspace.SYSTEM_KS);
            CQLStatement statement = getStatement(query, state).statement;
            statement.validate(state);
            ResultMessage result = statement.executeInternal(qState);
            if (result instanceof ResultMessage.Rows)
                return UntypedResultSet.create(((ResultMessage.Rows)result).result);
            else
                return null;
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException("Error validating " + query, e);
        }
    }

    public static UntypedResultSet resultify(String query, Row row)
    {
        return resultify(query, Collections.singletonList(row));
    }

    public static UntypedResultSet resultify(String query, List<Row> rows)
    {
        try
        {
            SelectStatement ss = (SelectStatement) getStatement(query, null).statement;
            ResultSet cqlRows = ss.process(rows);
            return UntypedResultSet.create(cqlRows);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e);
        }
    }

    public ResultMessage.Prepared prepare(String queryString, QueryState queryState)
    throws RequestValidationException
    {
        ClientState cState = queryState.getClientState();
        return prepare(queryString, cState, cState instanceof ThriftClientState);
    }

    public static ResultMessage.Prepared prepare(String queryString, ClientState clientState, boolean forThrift)
    throws RequestValidationException
    {
        ParsedStatement.Prepared prepared = getStatement(queryString, clientState);
        int boundTerms = prepared.statement.getBoundTerms();
        if (boundTerms > FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", boundTerms, FBUtilities.MAX_UNSIGNED_SHORT));
        assert boundTerms == prepared.boundNames.size();

        return storePreparedStatement(queryString, prepared, forThrift, clientState);
    }

    private static ResultMessage.Prepared storePreparedStatement(String queryString, ParsedStatement.Prepared prepared, boolean forThrift, ClientState clientState)
    throws InvalidRequestException
    {
        String keyspace = clientState.getRawKeyspace();
        // Concatenate the current keyspace so we don't mix prepared statements between keyspace (#5352).
        // (if the keyspace is null, queryString has to have a fully-qualified keyspace so it's fine.
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        long statementSize = measure(prepared.statement);
        // don't execute the statement if it's bigger than the allowed threshold
        if (statementSize > MAX_CACHE_PREPARED_MEMORY)
            throw new InvalidRequestException(String.format("Prepared statement of size %d bytes is larger than allowed maximum of %d bytes.",
                                                            statementSize,
                                                            MAX_CACHE_PREPARED_MEMORY));

        if (forThrift)
        {
            int statementId = toHash.hashCode();
            thriftPreparedStatements.put(statementId, prepared.statement);
            logger.trace(String.format("Stored prepared statement #%d with %d bind markers",
                                       statementId,
                                       prepared.statement.getBoundTerms()));
            return ResultMessage.Prepared.forThrift(statementId, prepared.boundNames);
        }
        else
        {
            MD5Digest statementId = MD5Digest.compute(toHash);
            preparedStatements.put(statementId, prepared.statement);
            logger.trace(String.format("Stored prepared statement %s with %d bind markers",
                                       statementId,
                                       prepared.statement.getBoundTerms()));

            // todo save the id and queryString of the preparared statement
            maybeLogQuery(1, statementId.bytes, queryString, clientState, prepared.statement, null);

            return new ResultMessage.Prepared(statementId, prepared);
        }
    }

    public ResultMessage processPrepared(Object statementId, CQLStatement statement, QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = options.getValues();
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && (statement.getBoundTerms() == 0)))
        {
            if (variables.size() != statement.getBoundTerms())
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.getBoundTerms(),
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero

            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i+1, variables.get(i));
        }

        // todo WIP, this is wasteful, avoid looping bb array twice
        int size = 0;
        for (ByteBuffer bb : variables)
            size += bb.limit();

        ByteBuffer variablesClone = ByteBuffer.allocate(size + 10);
        for (ByteBuffer bb : variables)
        {
            byte[] val = new byte[bb.limit()];
            variablesClone.putInt(val.length);  // so we know how many bytes to read
            variablesClone.put(val);            // actuall value
        }

        maybeLogQuery(2, ((MD5Digest)statementId).bytes, "", queryState.getClientState(), statement, variables);

        return processStatement(statement, queryState, options);
    }

    public ResultMessage processBatch(BatchStatement batch, QueryState queryState, BatchQueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        ClientState clientState = queryState.getClientState();
        batch.checkAccess(clientState);
        batch.validate(clientState);

        batch.executeWithPerStatementVariables(options.getConsistency(), queryState, options.getValues());
        return new ResultMessage.Void();
    }

    public static ParsedStatement.Prepared getStatement(String queryStr, ClientState clientState)
    throws RequestValidationException
    {
        Tracing.trace("Parsing {}", queryStr);
        ParsedStatement statement = parseStatement(queryStr);

        // Set keyspace for statement that require login
        if (statement instanceof CFStatement)
            ((CFStatement)statement).prepareKeyspace(clientState);

        Tracing.trace("Preparing statement");
        return statement.prepare();
    }

    public static ParsedStatement parseStatement(String queryStr) throws SyntaxException
    {
        try
        {
            // Lexer and parser
            CharStream stream = new ANTLRStringStream(queryStr);
            CqlLexer lexer = new CqlLexer(stream);
            TokenStream tokenStream = new CommonTokenStream(lexer);
            CqlParser parser = new CqlParser(tokenStream);

            // Parse the query string to a statement instance
            ParsedStatement statement = parser.query();

            // The lexer and parser queue up any errors they may have encountered
            // along the way, if necessary, we turn them into exceptions here.
            lexer.throwLastRecognitionError();
            parser.throwLastRecognitionError();

            return statement;
        }
        catch (RuntimeException re)
        {
            throw new SyntaxException(String.format("Failed parsing statement: [%s] reason: %s %s",
                                                    queryStr,
                                                    re.getClass().getSimpleName(),
                                                    re.getMessage()));
        }
        catch (RecognitionException e)
        {
            throw new SyntaxException("Invalid or malformed CQL query string: " + e.getMessage());
        }
    }

    private static long measure(Object key)
    {
        return key instanceof MeasurableForPreparedCache
             ? ((MeasurableForPreparedCache)key).measureForPreparedCache(meter)
             : meter.measureDeep(key);
    }

    /**
     * Checks if query should be logged based on whether it has been enabled
     * via JMX and if the query is on a non-system keyspace.
     * @param statementType
     * @param statementId
     * @param queryString query to be saved
     * @param client statement's ClientState to be used for execution.
     * @param statement parsed query used to retrieve keyspace
     */
    private static void maybeLogQuery(int statementType, byte [] statementId, String queryString, ClientState client, CQLStatement statement, List<ByteBuffer> vars)
    {

        QueryRecorder queryRecorder = StorageService.instance.getQueryRecorder();
        // dont log query if SS#queryRecorder is null as the logging hasn't yet been enabled.
        if (queryRecorder == null || isSystemOrTraceKS(statement, client))
            return;

        Integer frequency = queryRecorder.getFrequency();
        // when at the nth query, append query to the log
        if (querylogCounter.getAndIncrement() % frequency == 0)
        {
            // todo This is very expensive, it hurts the read/write path.
            //      possible rework is to pass strings along the chain of
            //      calls as a parameter to each method to avoid having to
            //      duplicate so many BBs.
            List<ByteBuffer> varsClone = new ArrayList<>();
            if (vars != null)
            {
                Iterator<ByteBuffer> ivars = vars.iterator();
                while (ivars.hasNext())
                    varsClone.add(ivars.next().duplicate());
            }
            queryRecorder.allocate((short)statementType, statementId, queryString, varsClone);
            logger.debug("Recorded query {}", queryString);
        }
    }

    /**
     * Check if a statement is issued on the 'system' or 'system_trace' keyspaces.
     * @param statement CQLStatement to be verified.
     * @param state statement's ClientState to be used for execution.
     * @return boolean representing whether a statement is issued on the system/trace keyspaces.
     */
    private static boolean isSystemOrTraceKS(CQLStatement statement, ClientState state)
    {
        try
        {
            return statement.isSystemOrTrace(state);
        }
        catch (UnauthorizedException uex)
        {
            throw new RuntimeException("Auth queries cannot be logged when accessing Cassandra anonymously.", uex);
        }
    }
}
