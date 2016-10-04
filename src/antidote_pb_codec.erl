%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(antidote_pb_codec).

-include("riak_pb.hrl").
-include("antidote_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([encode/2,
         decode/2,
         decode_response/1]).

-define(TYPE_COUNTER, counter).
-define(TYPE_SET, set).

encode(start_transaction, {Clock, Properties}) ->
    case Clock of
      ignore ->
              #apbstarttransaction{
                         properties = encode(txn_properties, Properties)};
      _ ->
              #apbstarttransaction{timestamp=Clock,
                         properties = encode(txn_properties, Properties)}
    end;

encode(txn_properties, _) ->
 %%TODO: Add more property paramaeters
 #apbtxnproperties{};

encode(abort_transaction, TxId) ->
    #apbaborttransaction{transaction_descriptor = TxId};

encode(commit_transaction, TxId) ->
    #apbcommittransaction{transaction_descriptor = TxId};

encode(update_objects, {Updates, TxId}) ->
   EncUpdates = lists:map(fun(Update) ->
                                  encode(update_op, Update) end,
                          Updates),
   #apbupdateobjects{ updates = EncUpdates, transaction_descriptor = TxId};

encode(update_op, {Object={_Key, Type, _Bucket}, Op, Param}) ->
    EncObject = encode(bound_object, Object),
    case Type of
        antidote_crdt_counter -> EncUp = encode(counter_update, {Op, Param}),
                                 #apbupdateop{boundobject=EncObject, optype = 'COUNTER', counterop = EncUp};
        antidote_crdt_orset -> SetUp = encode(set_update, {Op, Param}),
                               #apbupdateop{boundobject=EncObject, optype = 'SET', setop=SetUp};
        antidote_crdt_lwwreg -> EncUp = encode(reg_update, {Op, Param}),
                                #apbupdateop{boundobject=EncObject, optype = 'REG', regop = EncUp}

    end;

encode(static_update_objects, {Clock, Properties, Updates}) ->
    EncTransaction = encode(start_transaction, {Clock, Properties}),
    EncUpdates = lists:map(fun(Update) ->
                                  encode(update_op, Update) end,
                          Updates),
    #apbstaticupdateobjects{transaction = EncTransaction,
                            updates = EncUpdates};

encode(bound_object, {Key, Type, Bucket}) ->
    #apbboundobject{key=Key, type=encode(type,Type), bucket=Bucket};

encode(type, antidote_crdt_counter) -> 'COUNTER';
encode(type, antidote_crdt_orset) -> 'ORSET';
encode(type, antidote_crdt_lwwreg) -> 'LWWREG';

encode(reg_update, {assign, Value}) ->
    #apbregupdate{value=Value};

encode(counter_update, {increment, Amount}) ->
    #apbcounterupdate{inc = Amount};

encode(counter_update, {decrement, Amount}) ->
    #apbcounterupdate{inc= -Amount};

encode(set_update, {add, Elem}) ->
    #apbsetupdate{optype = 'ADD', adds = [term_to_binary(Elem)]};
encode(set_update, {add_all, Elems}) ->
    BinElems = lists:map(fun(Elem) ->
                                term_to_binary(Elem)
                        end,
                        Elems),
    #apbsetupdate{optype = 'ADD', adds = BinElems};
encode(set_update, {remove, Elem}) ->
    #apbsetupdate{optype= 'REMOVE', rems = [term_to_binary(Elem)]};
encode(set_update, {remove_all, Elems}) ->
    BinElems = lists:map(fun(Elem) ->
                                term_to_binary(Elem)
                        end,
                        Elems),
    #apbsetupdate{optype = 'REMOVE', rems = BinElems};


encode(read_objects, {Objects, TxId}) ->
    BoundObjects = lists:map(fun(Object) ->
                                     encode(bound_object, Object) end,
                             Objects),
    #apbreadobjects{boundobjects = BoundObjects, transaction_descriptor = TxId};


encode(static_read_objects, {Clock, Properties, Objects}) ->
    EncTransaction = encode(start_transaction, {Clock, Properties}),
    EncObjects = lists:map(fun(Object) ->
                                     encode(bound_object, Object) end,
                             Objects),
    #apbstaticreadobjects{transaction = EncTransaction,
                          objects = EncObjects};

encode(start_transaction_response, {error, Reason}) ->
    #apbstarttransactionresp{success=false, errorcode = encode(error_code, Reason)};

encode(start_transaction_response, {ok, TxId}) ->
    #apbstarttransactionresp{success=true, transaction_descriptor=term_to_binary(TxId)};

encode(operation_response, {error, Reason}) ->
    #apboperationresp{success=false, errorcode = encode(error_code, Reason)};

encode(operation_response, ok) ->
    #apboperationresp{success=true};

encode(commit_response, {error, Reason}) ->
    #apbcommitresp{success=false, errorcode = encode(error_code, Reason)};

encode(commit_response, {ok, CommitTime}) ->
    #apbcommitresp{success=true, commit_time= term_to_binary(CommitTime)};

encode(read_objects_response, {error, Reason}) ->
    #apbreadobjectsresp{success=false, errorcode = encode(error_code, Reason)};

encode(read_objects_response, {ok, Results}) ->
    EncResults = lists:map(fun(R) ->
                                   encode(read_object_resp, R) end,
                           Results),
    #apbreadobjectsresp{success=true, objects = EncResults};

encode(read_object_resp, {{_Key, antidote_crdt_lwwreg, _Bucket}, Val}) ->
    #apbreadobjectresp{reg=#apbgetregresp{value=term_to_binary(Val)}};

encode(read_object_resp, {{_Key, antidote_crdt_counter, _Bucket}, Val}) ->
    #apbreadobjectresp{counter=#apbgetcounterresp{value=Val}};

encode(read_object_resp, {{_Key, antidote_crdt_orset, _Bucket}, Val}) ->
    #apbreadobjectresp{set=#apbgetsetresp{value=Val}};

encode(static_read_objects_response, {ok, Results, CommitTime}) ->
    #apbstaticreadobjectsresp{
       objects = encode(read_objects_response, {ok, Results}),
       committime = encode(commit_response, {ok, CommitTime})};

%% Add new error codes
encode(error_code, unknown) -> 0;
encode(error_code, timeout) -> 1;
encode(error_code, _Other) -> 0;

encode(_Other, _) ->
    erlang:error("Incorrect operation/Not yet implemented").

decode(txn_properties, _Properties) ->
    {};
decode(bound_object, #apbboundobject{key = Key, type=Type, bucket=Bucket}) ->
    {Key, decode(type, Type), Bucket};

decode(type, 'COUNTER') -> antidote_crdt_counter;
decode(type, 'ORSET') -> antidote_crdt_orset;
decode(type, 'LWWREG') -> antidote_crdt_lwwreg;
decode(error_code, 0) -> unknown;
decode(error_code, 1) -> timeout;

decode(update_object, #apbupdateop{boundobject = Object, optype = OpType, counterop = CounterOp, setop = SetOp,
                regop = RegOp}) ->
    {Op, OpParam} = case OpType of
                 1 ->
                     decode(counter_update, CounterOp);
                 2 ->
                     decode(set_update, SetOp);
                 3 ->
                     decode(reg_update, RegOp)
    end,
    {decode(bound_object, Object), Op, OpParam};
decode(reg_update, #apbregupdate{value =Value}) ->
    {assign, Value};
decode(counter_update, #apbcounterupdate{inc = I}) ->
    case I of
        undefined -> {increment, 1};
        I ->  {increment, I} % negative value for I indicates decrement
    end;

decode(set_update, #apbsetupdate{optype = OpType, adds = A, rems = R}) ->
  case OpType  of
      'ADD' ->
          OpsAdd =  case A of
                      undefined -> [];
                      AddElems when is_list(AddElems)-> {add_all, AddElems}
                    end,
          OpsAdd;
      'REMOVE' ->
          OpsRem = case R of
                      undefined -> [];
                      Elems when is_list(Elems) -> {remove_all, Elems}
                    end,
          OpsRem
      end;

decode(_Other, _) ->
    erlang:error("Unknown message").

decode_response(#apboperationresp{success = true}) ->
    {opresponse, ok};
decode_response(#apboperationresp{success = false, errorcode = Reason})->
    {error, decode(error_code, Reason)};
decode_response(#apbstarttransactionresp{success=true,
                                         transaction_descriptor = TxId}) ->
    {start_transaction, TxId};
decode_response(#apbstarttransactionresp{success=false, errorcode = Reason}) ->
    {error, decode(error_code, Reason)};
decode_response(#apbcommitresp{success=true, commit_time = TimeStamp}) ->
    {commit_transaction, TimeStamp};
decode_response(#apbcommitresp{success=false, errorcode = Reason}) ->
    {error, decode(error_code, Reason)};
decode_response(#apbreadobjectsresp{success=false, errorcode=Reason}) ->
    {error, decode(error_code, Reason)};
decode_response(#apbreadobjectsresp{success=true, objects = Objects}) ->
    Resps = lists:map(fun(O) ->
                              decode_response(O) end,
                      Objects),
    {read_objects, Resps};
decode_response(#apbreadobjectresp{counter = #apbgetcounterresp{value = Val}}) ->
    {counter, Val};
decode_response(#apbreadobjectresp{set = #apbgetsetresp{value = Val}}) ->
    Res = lists:map(fun(Elem) ->
                              binary_to_term(Elem)
                    end,
                    Val),
    {set, Res};
decode_response(#apbreadobjectresp{reg = #apbgetregresp{value = Val}}) ->
    {reg, erlang:binary_to_term(Val)};
decode_response(#apbstaticreadobjectsresp{objects = Objects,
                                          committime = CommitTime}) ->
    {read_objects, Values} = decode_response(Objects),
    {commit_transaction, TimeStamp} = decode_response(CommitTime),
    {static_read_objects_resp, Values, TimeStamp};
decode_response(Other) ->
    erlang:error("Unexpected message: ~p",[Other]).



-ifdef(TEST).

%% Tests encode and decode
start_transaction_test() ->
    Clock = term_to_binary(ignore),
    Properties = {},
    EncRecord = antidote_pb_codec:encode(start_transaction,
                                         {Clock, Properties}),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg,apbstarttransaction)),
    ?assertMatch(ignore, binary_to_term(Msg#apbstarttransaction.timestamp)),
    ?assertMatch(Properties,
                 antidote_pb_codec:decode(txn_properties,
                                          Msg#apbstarttransaction.properties)).

read_transaction_test() ->
    Objects = [{<<"key1">>, antidote_crdt_counter, <<"bucket1">>},
               {<<"key2">>, antidote_crdt_orset, <<"bucket2">>}],
    TxId = term_to_binary({12}),
         %% Dummy value, structure of TxId is opaque to client
    EncRecord = antidote_pb_codec:encode(read_objects, {Objects, TxId}),
    ?assertMatch(true, is_record(EncRecord, apbreadobjects)),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg, apbreadobjects)),
    DecObjects = lists:map(fun(O) ->
                                antidote_pb_codec:decode(bound_object, O) end,
                           Msg#apbreadobjects.boundobjects),
    ?assertMatch(Objects, DecObjects),
    %% Test encoding error
    ErrEnc = antidote_pb_codec:encode(read_objects_response,
                                      {error, someerror}),
    [ErrMsgCode,ErrMsgData] = riak_pb_codec:encode(ErrEnc),
    ErrMsg = riak_pb_codec:decode(ErrMsgCode,list_to_binary(ErrMsgData)),
    ?assertMatch({error, unknown},
                 antidote_pb_codec:decode_response(ErrMsg)),

    %% Test encoding results
    Results = [1, [2]],
    ResEnc = antidote_pb_codec:encode(read_objects_response,
                                      {ok, lists:zip(Objects, Results)}
                                     ),
    [ResMsgCode, ResMsgData] = riak_pb_codec:encode(ResEnc),
    ResMsg = riak_pb_codec:decode(ResMsgCode, list_to_binary(ResMsgData)),
    ?assertMatch({read_objects, [{counter, 1}, {set, [2]}]},
                 antidote_pb_codec:decode_response(ResMsg)).

update_types_test() ->
    Updates = [ {{<<"1">>, antidote_crdt_counter, <<"2">>}, increment , 1},
                {{<<"2">>, antidote_crdt_counter, <<"2">>}, increment , 1},
                {{<<"a">>, antidote_crdt_orset, <<"2">>}, add , 3},
                {{<<"b">>, antidote_crdt_counter, <<"2">>}, increment , 2},
                {{<<"c">>, antidote_crdt_orset, <<"2">>}, add, 4},
                {{<<"a">>, antidote_crdt_orset, <<"2">>}, add_all , [5,6]}
              ],
    TxId = term_to_binary({12}),
         %% Dummy value, structure of TxId is opaque to client
    EncRecord = antidote_pb_codec:encode(update_objects, {Updates, TxId}),
    ?assertMatch(true, is_record(EncRecord, apbupdateobjects)),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg, apbupdateobjects)),
    DecUpdates = lists:map(fun(O) ->
                                antidote_pb_codec:decode(update_object, O) end,
                        Msg#apbupdateobjects.updates),
    ?assertMatch(Updates, DecUpdates).

error_messages_test() ->
    EncRecord1 = antidote_pb_codec:encode(start_transaction_response,
                                          {error, someerror}),
    [MsgCode1, MsgData1] = riak_pb_codec:encode(EncRecord1),
    Msg1 = riak_pb_codec:decode(MsgCode1, list_to_binary(MsgData1)),
    Resp1 = antidote_pb_codec:decode_response(Msg1),
    ?assertMatch(Resp1, {error, unknown}),

    EncRecord2 = antidote_pb_codec:encode(operation_response,
                                          {error, someerror}),
    [MsgCode2, MsgData2] = riak_pb_codec:encode(EncRecord2),
    Msg2 = riak_pb_codec:decode(MsgCode2, list_to_binary(MsgData2)),
    Resp2 = antidote_pb_codec:decode_response(Msg2),
    ?assertMatch(Resp2, {error, unknown}),

    EncRecord3 = antidote_pb_codec:encode(read_objects_response,
                                          {error, someerror}),
    [MsgCode3, MsgData3] = riak_pb_codec:encode(EncRecord3),
    Msg3 = riak_pb_codec:decode(MsgCode3, list_to_binary(MsgData3)),
    Resp3 = antidote_pb_codec:decode_response(Msg3),
    ?assertMatch(Resp3, {error, unknown}).

-endif.
