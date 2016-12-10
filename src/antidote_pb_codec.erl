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
  decode_response/1, encode_read_objects/2, decode_bound_object/1, encode_update_objects/2, decode_update_op/1]).

-define(TYPE_COUNTER, counter).
-define(TYPE_SET, set).


-define(assert_binary(X), case is_binary(X) of true -> ok; false -> throw({not_binary, X}) end).
-define(assert_all_binary(Xs), [?assert_binary(X) || X <- Xs]).

% general encode function
encode(start_transaction, {Clock, Properties}) ->
  encode_start_transaction(Clock, Properties);
encode(txn_properties, Props) ->
  encode_txn_properties(Props);
encode(abort_transaction, TxId) ->
  encode_abort_transaction(TxId);
encode(commit_transaction, TxId) ->
  encode_commit_transaction(TxId);
encode(update_objects, {Updates, TxId}) ->
  encode_update_objects(Updates, TxId);
encode(update_op, {Object, Op, Param}) ->
  encode_update_op(Object, Op, Param);
encode(static_update_objects, {Clock, Properties, Updates}) ->
  encode_static_update_objects(Clock, Properties, Updates);
encode(bound_object, {Key, Type, Bucket}) ->
  encode_bound_object(Key, Type, Bucket);
encode(type, Type) ->
  encode_type(Type);
encode(reg_update, Update) ->
  encode_reg_update(Update);
encode(counter_update, Update) ->
  encode_counter_update(Update);
encode(set_update, Update) ->
  encode_set_update(Update);
encode(read_objects, {Objects, TxId}) ->
  encode_read_objects(Objects, TxId);
encode(static_read_objects, {Clock, Properties, Objects}) ->
  encode_static_read_objects(Clock, Properties, Objects);
encode(start_transaction_response, Resp) ->
  encode_start_transaction_response(Resp);
encode(operation_response, Resp) ->
  encode_operation_response(Resp);
encode(commit_response, Resp) ->
  encode_commit_response(Resp);
encode(read_objects_response, Resp) ->
  encode_read_objects_response(Resp);
encode(read_object_resp, Resp) ->
  encode_read_object_resp(Resp);
encode(static_read_objects_response, {ok, Results, CommitTime}) ->
  encode_static_read_objects_response(Results, CommitTime);
encode(error_code, Code) ->
  encode_error_code(Code);
encode(_Other, _) ->
    erlang:error("Incorrect operation/Not yet implemented").

% general decode function
decode(txn_properties, Properties) ->
  decode_txn_properties(Properties);
decode(bound_object, Obj) ->
  decode_bound_object(Obj);
decode(type, Type) ->
  decode_type(Type);
decode(error_code, Code) ->
  decode_error_code(Code);
decode(update_object, Obj) ->
  decode_update_op(Obj);
decode(reg_update, Update) ->
  decode_reg_update(Update);
decode(counter_update, Update) ->
  decode_counter_update(Update);
decode(set_update, Update) ->
  decode_set_update(Update);
decode(_Other, _) ->
    erlang:error("Unknown message").



%%%%%%%%%%%%%%%%%%%%%%%%%%%
% error codes

encode_error_code(unknown) -> 0;
encode_error_code(timeout) -> 1;
encode_error_code(_Other) -> 0.

decode_error_code(0) -> unknown;
decode_error_code(1) -> timeout.



%%%%%%%%%%%%%%%%%%%%%%%
% Transactions

encode_start_transaction(Clock, Properties) ->
  case Clock of
    ignore ->
      #apbstarttransaction{
        properties = encode_txn_properties(Properties)};
    _ ->
      #apbstarttransaction{timestamp = Clock,
        properties = encode_txn_properties(Properties)}
  end.


encode_commit_transaction(TxId) ->
  #apbcommittransaction{transaction_descriptor = TxId}.

encode_abort_transaction(TxId) ->
  #apbaborttransaction{transaction_descriptor = TxId}.


encode_txn_properties(_Props) ->
  %%TODO: Add more property parameters
  #apbtxnproperties{}.

decode_txn_properties(_Properties) ->
  {}.



%%%%%%%%%%%%%%%%%%%%%
%% Updates

% bound objects

encode_bound_object({Key, Type, Bucket}) ->
  encode_bound_object(Key, Type, Bucket).
encode_bound_object(Key, Type, Bucket) ->
  #apbboundobject{key = Key, type = encode_type(Type), bucket = Bucket}.

decode_bound_object(Obj) ->
  #apbboundobject{key = Key, type = Type, bucket = Bucket} = Obj,
  {Key, decode_type(Type), Bucket}.


% static_update_objects

encode_static_update_objects(Clock, Properties, Updates) ->
  EncTransaction = encode_start_transaction(Clock, Properties),
  EncUpdates = lists:map(fun(Update) ->
    encode_update_op(Update) end,
    Updates),
  #apbstaticupdateobjects{transaction = EncTransaction,
    updates = EncUpdates}.


decode_update_op(Obj) ->
  #apbupdateop{boundobject = Object, operation = Operation}=Obj,
  {Op, OpParam} = decode_update_operation(Operation),
  {decode_bound_object(Object), Op, OpParam}.


encode_update_op({Object, Op, Param}) ->
  encode_update_op(Object, Op, Param).
encode_update_op(Object, Op, Param) ->
  {_Key, Type, _Bucket} = Object,
  EncObject = encode_bound_object(Object),
  Operation = encode_update_operation(Type, {Op, Param}),
  #apbupdateop{boundobject = EncObject, operation = Operation}.



encode_update_objects(Updates, TxId) ->
  EncUpdates = lists:map(fun(Update) ->
    encode_update_op(Update) end,
    Updates),
  #apbupdateobjects{updates = EncUpdates, transaction_descriptor = TxId}.




%%%%%%%%%%%%%%%%%%%%%%%%
%% Responses

encode_static_read_objects_response(Results, CommitTime) ->
  #apbstaticreadobjectsresp{
    objects = encode_read_objects_response({ok, Results}),
    committime = encode_commit_response({ok, CommitTime})}.


encode_read_objects_response({error, Reason}) ->
    #apbreadobjectsresp{success=false, errorcode = encode_error_code(Reason)};
encode_read_objects_response({ok, Results}) ->
    EncResults = lists:map(fun(R) ->
                                   encode_read_object_resp(R) end,
                           Results),
    #apbreadobjectsresp{success=true, objects = EncResults}.


encode_start_transaction_response({error, Reason}) ->
  #apbstarttransactionresp{success = false, errorcode = encode_error_code(Reason)};
encode_start_transaction_response({ok, TxId}) ->
  #apbstarttransactionresp{success = true, transaction_descriptor = term_to_binary(TxId)}.

encode_operation_response({error, Reason}) ->
    #apboperationresp{success=false, errorcode = encode_error_code(Reason)};
encode_operation_response(ok) ->
    #apboperationresp{success=true}.

encode_commit_response({error, Reason}) ->
    #apbcommitresp{success=false, errorcode = encode_error_code(Reason)};

encode_commit_response({ok, CommitTime}) ->
    #apbcommitresp{success=true, commit_time= term_to_binary(CommitTime)}.

decode_response(#apboperationresp{success = true}) ->
    {opresponse, ok};
decode_response(#apboperationresp{success = false, errorcode = Reason})->
    {error, decode_error_code(Reason)};
decode_response(#apbstarttransactionresp{success=true,
                                         transaction_descriptor = TxId}) ->
    {start_transaction, TxId};
decode_response(#apbstarttransactionresp{success=false, errorcode = Reason}) ->
    {error, decode_error_code(Reason)};
decode_response(#apbcommitresp{success=true, commit_time = TimeStamp}) ->
    {commit_transaction, TimeStamp};
decode_response(#apbcommitresp{success=false, errorcode = Reason}) ->
    {error, decode_error_code(Reason)};
decode_response(#apbreadobjectsresp{success=false, errorcode=Reason}) ->
    {error, decode_error_code(Reason)};
decode_response(#apbreadobjectsresp{success=true, objects = Objects}) ->
    Resps = lists:map(fun(O) ->
                              decode_response(O) end,
                      Objects),
    {read_objects, Resps};
decode_response(#apbreadobjectresp{}=ReadObjectResp) ->
  decode_read_object_resp(ReadObjectResp);
decode_response(#apbstaticreadobjectsresp{objects = Objects,
                                          committime = CommitTime}) ->
    {read_objects, Values} = decode_response(Objects),
    {commit_transaction, TimeStamp} = decode_response(CommitTime),
    {static_read_objects_resp, Values, TimeStamp};
decode_response(Other) ->
    erlang:error("Unexpected message: ~p",[Other]).

%%%%%%%%%%%%%%%%%%%%%%
%% Reading objects


encode_static_read_objects(Clock, Properties, Objects) ->
  EncTransaction = encode_start_transaction(Clock, Properties),
  EncObjects = lists:map(fun(Object) ->
    encode_bound_object(Object) end,
    Objects),
  #apbstaticreadobjects{transaction = EncTransaction,
    objects = EncObjects}.

encode_read_objects(Objects, TxId) ->
  BoundObjects = lists:map(fun(Object) ->
    encode_bound_object(Object) end,
    Objects),
  #apbreadobjects{boundobjects = BoundObjects, transaction_descriptor = TxId}.

%%%%%%%%%%%%%%%%%%%
%% Crdt types

%%COUNTER = 3;
%%ORSET = 4;
%%LWWREG = 5;
%%MVREG = 6;
%%INTEGER = 7;
%%GMAP = 8;
%%AWMAP = 9;
%%RWSET = 10;

encode_type(antidote_crdt_counter) -> 'COUNTER';
encode_type(antidote_crdt_fat_counter) -> 'FATCOUNTER';
encode_type(antidote_crdt_orset) -> 'ORSET';
encode_type(antidote_crdt_lwwreg) -> 'LWWREG';
encode_type(antidote_crdt_mvreg) -> 'MVREG';
encode_type(antidote_crdt_integer) -> 'INTEGER';
encode_type(antidote_crdt_gmap) -> 'GMAP';
encode_type(antidote_crdt_map_aw) -> 'AWMAP';
encode_type(antidote_crdt_set_rw) -> 'RWSET';
encode_type(antidote_crdt_map_rr) -> 'RRMAP'.


decode_type('COUNTER') -> antidote_crdt_counter;
decode_type('FATCOUNTER') -> antidote_crdt_fat_counter;
decode_type('ORSET') -> antidote_crdt_orset;
decode_type('LWWREG') -> antidote_crdt_lwwreg;
decode_type('MVREG') -> antidote_crdt_mvreg;
decode_type('INTEGER') -> antidote_crdt_integer;
decode_type('GMAP') -> antidote_crdt_gmap;
decode_type('AWMAP') -> antidote_crdt_map_aw;
decode_type('RWSET') -> antidote_crdt_set_rw;
decode_type('RRMAP') -> antidote_crdt_map_rr;


%%%%%%%%%%%%%%%%%%%%%%
% CRDT operations


encode_update_operation(antidote_crdt_counter, Op_Param) ->
  #apbupdateoperation{counterop = encode_counter_update(Op_Param)};
encode_update_operation(antidote_crdt_orset, Op_Param) ->
  #apbupdateoperation{setop = encode_set_update(Op_Param)};
encode_update_operation(antidote_crdt_set_rw, Op_Param) ->
  #apbupdateoperation{setop = encode_set_update(Op_Param)};
encode_update_operation(antidote_crdt_lwwreg, Op_Param) ->
  #apbupdateoperation{regop = encode_reg_update(Op_Param)};
encode_update_operation(antidote_crdt_mvreg, Op_Param) ->
  #apbupdateoperation{regop = encode_reg_update(Op_Param)};
encode_update_operation(antidote_crdt_integer, Op_Param) ->
  #apbupdateoperation{integerop = encode_integer_update(Op_Param)};
encode_update_operation(antidote_crdt_gmap, Op_Param) ->
  #apbupdateoperation{mapop = encode_map_update(Op_Param)};
encode_update_operation(antidote_crdt_map_aw, Op_Param) ->
  #apbupdateoperation{mapop = encode_map_update(Op_Param)};
encode_update_operation(Type, _Op) ->
  throw({invalid_type, Type}).

decode_update_operation(#apbupdateoperation{counterop = Op}) when Op /= undefined ->
  decode_counter_update(Op);
decode_update_operation(#apbupdateoperation{setop = Op}) when Op /= undefined ->
  decode_set_update(Op);
decode_update_operation(#apbupdateoperation{regop = Op}) when Op /= undefined ->
  decode_reg_update(Op);
decode_update_operation(#apbupdateoperation{integerop = Op}) when Op /= undefined ->
  decode_integer_update(Op);
decode_update_operation(#apbupdateoperation{mapop = Op}) when Op /= undefined ->
  decode_map_update(Op).

%%decode_update_operation(#apbupdateoperation{mapop = Op}) when Op /= undefined ->
%%  decode_map_update(Op);
%%decode_update_operation(#apbupdateoperation{resetop = Op}) when Op /= undefined ->
%%  decode_reset_update(Op).

% general encoding of CRDT responses

encode_read_object_resp({{_Key, Type, _Bucket}, Val}) ->
  encode_read_object_resp(Type, Val).

encode_read_object_resp(antidote_crdt_lwwreg, Val) ->
    #apbreadobjectresp{reg=#apbgetregresp{value=Val}};
encode_read_object_resp(antidote_crdt_mvreg, Vals) ->
    #apbreadobjectresp{mvreg = #apbgetmvregresp{values = Vals}};
encode_read_object_resp(antidote_crdt_counter, Val) ->
    #apbreadobjectresp{counter=#apbgetcounterresp{value=Val}};
encode_read_object_resp(antidote_crdt_orset, Val) ->
    #apbreadobjectresp{set=#apbgetsetresp{value=Val}};
encode_read_object_resp(antidote_crdt_set_rw, Val) ->
    #apbreadobjectresp{set=#apbgetsetresp{value=Val}};
encode_read_object_resp(antidote_crdt_integer, Val) ->
    #apbreadobjectresp{int = #apbgetintegerresp{value = Val}};
encode_read_object_resp(antidote_crdt_gmap, Val) ->
    #apbreadobjectresp{map = encode_map_get_resp(Val)};
encode_read_object_resp(antidote_crdt_map_aw, Val) ->
    #apbreadobjectresp{map = encode_map_get_resp(Val)}.

% TODO why does this use counter instead of antidote_crdt_counter etc.?
decode_read_object_resp(#apbreadobjectresp{counter = #apbgetcounterresp{value = Val}}) ->
    {counter, Val};
decode_read_object_resp(#apbreadobjectresp{set = #apbgetsetresp{value = Val}}) ->
    {set, Val};
decode_read_object_resp(#apbreadobjectresp{reg = #apbgetregresp{value = Val}}) ->
    {reg, Val};
decode_read_object_resp(#apbreadobjectresp{mvreg = #apbgetmvregresp{values = Vals}}) ->
    {mvreg, Vals};
decode_read_object_resp(#apbreadobjectresp{int = #apbgetintegerresp{value = Val}}) ->
    {integer, Val};
decode_read_object_resp(#apbreadobjectresp{map = MapResp=#apbgetmapresp{}}) ->
    {map, decode_map_get_resp(MapResp)}.

% set updates

encode_set_update({add, Elem}) ->
    ?assert_binary(Elem),
    #apbsetupdate{optype = 'ADD', adds = [Elem]};
encode_set_update({add_all, Elems}) ->
    ?assert_all_binary(Elems),
    #apbsetupdate{optype = 'ADD', adds = Elems};
encode_set_update({remove, Elem}) ->
    ?assert_binary(Elem),
    #apbsetupdate{optype= 'REMOVE', rems = [Elem]};
encode_set_update({remove_all, Elems}) ->
    ?assert_all_binary(Elems),
    #apbsetupdate{optype = 'REMOVE', rems = Elems}.

decode_set_update(Update) ->
  #apbsetupdate{optype = OpType, adds = A, rems = R} = Update,
  case OpType of
    'ADD' ->
      case A of
        undefined -> [];
        [Elem] -> {add, Elem};
        AddElems when is_list(AddElems) -> {add_all, AddElems}
      end;
    'REMOVE' ->
      case R of
        undefined -> [];
        [Elem] -> {remove, Elem};
        Elems when is_list(Elems) -> {remove_all, Elems}
      end
  end.

% counter updates

encode_counter_update({increment, Amount}) ->
  #apbcounterupdate{inc = Amount};
encode_counter_update({decrement, Amount}) ->
  #apbcounterupdate{inc = -Amount}.


decode_counter_update(Update) ->
  #apbcounterupdate{inc = I} = Update,
  case I of
    undefined -> {increment, 1};
    I -> {increment, I} % negative value for I indicates decrement
  end.


% register updates

encode_reg_update(Update) ->
  {assign, Value} = Update,
  #apbregupdate{value = Value}.


decode_reg_update(Update) ->
  #apbregupdate{value = Value} = Update,
  {assign, Value}.

% integer updates

encode_integer_update({set, Value}) ->
  #apbintegerupdate{set = Value};
encode_integer_update({increment, Value}) ->
  #apbintegerupdate{inc = Value}.

decode_integer_update(#apbintegerupdate{set=Value}) when is_integer(Value) ->
  {set, Value};
decode_integer_update(#apbintegerupdate{inc=Value}) when is_integer(Value) ->
  {increment, Value}.

% map updates

encode_map_update({update, Ops}) when is_list(Ops) ->
  encode_map_update({batch, {Ops, []}});
encode_map_update({update, Op}) ->
  encode_map_update({batch, {[Op], []}});
encode_map_update({remove, Keys}) when is_list(Keys) ->
  encode_map_update({batch, {[], Keys}});
encode_map_update({remove, Key}) ->
  encode_map_update({batch, {[], [Key]}});
encode_map_update({batch, {Updates, RemovedKeys}}) ->
  UpdatesEnc = [encode_map_nested_update(U) || U <- Updates],
  RemovedKeysEnc = [encode_map_key(K) || K <- RemovedKeys],
  #apbmapupdate{updates = UpdatesEnc, removedkeys = RemovedKeysEnc}.


decode_map_update(#apbmapupdate{updates = [Update], removedkeys = []}) ->
  {update, decode_map_nested_update(Update)};
decode_map_update(#apbmapupdate{updates = Updates, removedkeys = []}) ->
  {update, [decode_map_nested_update(U) || U <- Updates]};
decode_map_update(#apbmapupdate{updates = [], removedkeys = [Key]}) ->
  {remove, decode_map_key(Key)};
decode_map_update(#apbmapupdate{updates = [], removedkeys = Keys}) ->
  {remove, [decode_map_key(K) || K <- Keys]};
decode_map_update(#apbmapupdate{updates = Updates, removedkeys = Keys}) ->
  {batch, {[decode_map_nested_update(U) || U <- Updates], [decode_map_key(K) || K <- Keys]}}.


encode_map_nested_update({{Key,Type}, Update}) ->
  #apbmapnestedupdate{
    key = encode_map_key({Key,Type}),
    update = encode_update_operation(Type, Update)
  }.

decode_map_nested_update(#apbmapnestedupdate{key = KeyEnc, update = UpdateEnc}) ->
  {Key, Type} = decode_map_key(KeyEnc),
  Update = decode_update_operation(UpdateEnc),
  {{Key, Type}, Update}.

encode_map_key({Key, Type}) ->
  ?assert_binary(Key),
  #apbmapkey{
    key = Key,
    type = encode_type(Type)
  }.

decode_map_key(#apbmapkey{key = Key, type = Type}) ->
  {Key, decode_type(Type)}.

% map responses

encode_map_get_resp(Entries) ->
  #apbgetmapresp{entries = [encode_map_entry(E) || E <- Entries]}.

decode_map_get_resp(#apbgetmapresp{entries = Entries}) ->
  [decode_map_entry(E) || E <- Entries].

encode_map_entry({{Key,Type}, Val}) ->
  #apbmapentry{
    key = encode_map_key({Key, Type}),
    value = encode_read_object_resp(Type, Val)
  }.

decode_map_entry(#apbmapentry{key = KeyEnc, value = ValueEnc}) ->
  {Key, Type} = decode_map_key(KeyEnc),
  {_Tag, Value} = decode_read_object_resp(ValueEnc),
  {{Key, Type}, Value}.





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
    EncRecord = antidote_pb_codec:encode_read_objects(Objects, TxId),
    ?assertMatch(true, is_record(EncRecord, apbreadobjects)),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg, apbreadobjects)),
    DecObjects = lists:map(fun(O) ->
                                antidote_pb_codec:decode_bound_object(O) end,
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
    Results = [1, [<<"a">>,<<"b">>]],
    ResEnc = antidote_pb_codec:encode(read_objects_response,
                                      {ok, lists:zip(Objects, Results)}
                                     ),
    [ResMsgCode, ResMsgData] = riak_pb_codec:encode(ResEnc),
    ResMsg = riak_pb_codec:decode(ResMsgCode, list_to_binary(ResMsgData)),
    ?assertMatch({read_objects, [{counter, 1}, {set, [<<"a">>,<<"b">>]}]},
                 antidote_pb_codec:decode_response(ResMsg)).

update_types_test() ->
    Updates = [ {{<<"1">>, antidote_crdt_counter, <<"2">>}, increment , 1},
                {{<<"2">>, antidote_crdt_counter, <<"2">>}, increment , 1},
                {{<<"a">>, antidote_crdt_orset, <<"2">>}, add , <<"3">>},
                {{<<"b">>, antidote_crdt_counter, <<"2">>}, increment , 2},
                {{<<"c">>, antidote_crdt_orset, <<"2">>}, add, <<"4">>},
                {{<<"a">>, antidote_crdt_orset, <<"2">>}, add_all , [<<"5">>,<<"6">>]}
              ],
    TxId = term_to_binary({12}),
         %% Dummy value, structure of TxId is opaque to client
    EncRecord = antidote_pb_codec:encode_update_objects(Updates, TxId),
    ?assertMatch(true, is_record(EncRecord, apbupdateobjects)),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg, apbupdateobjects)),
    DecUpdates = lists:map(fun(O) ->
                                antidote_pb_codec:decode_update_op(O) end,
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

-define(TestCrdtOperationCodec(Type, Op, Param),
    ?assertEqual(
      {{<<"key">>, Type, <<"bucket">>}, Op, Param},
      decode_update_op(encode_update_op({<<"key">>, Type, <<"bucket">>}, Op, Param)))
).

-define(TestCrdtResponseCodec(Type, ExpectedType, Val),
    ?assertEqual(
      {ExpectedType, Val},
      decode_read_object_resp(encode_read_object_resp(Type, Val)))
).

crdt_encode_decode_test() ->
  %% encoding the following operations and decoding them again, should give the same result

  % Counter
  ?TestCrdtOperationCodec(antidote_crdt_counter, increment, 1),
  ?TestCrdtResponseCodec(antidote_crdt_counter, counter, 42),

  % lww-register
  ?TestCrdtOperationCodec(antidote_crdt_lwwreg, assign, <<"hello">>),
  ?TestCrdtResponseCodec(antidote_crdt_lwwreg, reg, <<"blub">>),


  % mv-register
  ?TestCrdtOperationCodec(antidote_crdt_mvreg, assign, <<"hello">>),
  ?TestCrdtResponseCodec(antidote_crdt_mvreg, mvreg, [<<"a">>, <<"b">>, <<"c">>]),

  % set
  ?TestCrdtOperationCodec(antidote_crdt_orset, add, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_orset, add_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtOperationCodec(antidote_crdt_orset, remove, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_orset, remove_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtResponseCodec(antidote_crdt_orset, set, [<<"a">>, <<"b">>, <<"c">>]),

  % same for remove wins set:
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, add, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, add_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, remove, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, remove_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtResponseCodec(antidote_crdt_set_rw, set, [<<"a">>, <<"b">>, <<"c">>]),

  % integer
  ?TestCrdtOperationCodec(antidote_crdt_integer, set, 13),
  ?TestCrdtOperationCodec(antidote_crdt_integer, increment, 7),
  ?TestCrdtResponseCodec(antidote_crdt_integer, integer, 123),

  % map
  ?TestCrdtOperationCodec(antidote_crdt_map_aw, update, {{<<"key">>, antidote_crdt_integer}, {set, 42}}),
  ?TestCrdtOperationCodec(antidote_crdt_map_aw, update, [
    {{<<"a">>, antidote_crdt_integer}, {set, 42}},
    {{<<"b">>, antidote_crdt_orset}, {add, <<"x">>}}]),
  ?TestCrdtOperationCodec(antidote_crdt_map_aw, remove, {<<"key">>, antidote_crdt_integer}),
  ?TestCrdtOperationCodec(antidote_crdt_map_aw, remove, [
    {<<"a">>, antidote_crdt_integer},
    {<<"b">>, antidote_crdt_integer}]),
  ?TestCrdtOperationCodec(antidote_crdt_map_aw, batch, {
    [ {{<<"a">>, antidote_crdt_integer}, {set, 42}},
      {{<<"b">>, antidote_crdt_orset}, {add, <<"x">>}}],
    [ {<<"a">>, antidote_crdt_integer},
      {<<"b">>, antidote_crdt_integer}]}),

  ?TestCrdtResponseCodec(antidote_crdt_map_aw, map, [
    {{<<"a">>, antidote_crdt_integer}, 42}
  ]),

  % gmap
  ?TestCrdtOperationCodec(antidote_crdt_gmap, update, {{<<"key">>, antidote_crdt_integer}, {set, 42}}),
  ?TestCrdtOperationCodec(antidote_crdt_gmap, update, [
    {{<<"a">>, antidote_crdt_integer}, {set, 42}},
    {{<<"b">>, antidote_crdt_orset}, {add, <<"x">>}}]),
  ?TestCrdtResponseCodec(antidote_crdt_gmap, map, [
    {{<<"a">>, antidote_crdt_integer}, 42}
  ]),



  ok.



-endif.




