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
	 decode_json/1,
         decode_response/1]).

-define(TYPE_COUNTER, counter).
-define(TYPE_SET, set).

encode(start_transaction, {Clock, Properties, proto_buf}) ->
    #apbstarttransaction{timestamp=Clock,
                         properties = encode(txn_properties, Properties)};

encode(start_transaction, {Clock, Properties, json}) ->
    JReq = [{start_transaction, [vectorclock:to_json(Clock), encode_json(txn_properties, Properties)]}],
    #apbjsonrequest{value=jsx:encode(JReq)};

encode(txn_properties, _) ->
 %%TODO: Add more property paramaeters
 #apbtxnproperties{};


encode(abort_transaction, {TxId, proto_buf}) ->
    #apbaborttransaction{transaction_descriptor = TxId};

encode(abort_transaction, {TxId,json}) ->
    JReq = [{abort_transaction, json_utilities:txid_to_json(TxId)}],
    #apbjsonrequest{value=jsx:encode(JReq)};

encode(commit_transaction, {TxId, proto_buf}) ->
    #apbcommittransaction{transaction_descriptor = TxId};

encode(commit_transaction, {TxId,json}) ->
    JReq = [{commit_transaction, json_utilities:txid_to_json(TxId)}],
    #apbjsonrequest{value=jsx:encode(JReq)};

encode(update_objects, {Updates, TxId, proto_buf}) ->
    EncUpdates = lists:map(fun(Update) ->
				   encode(update_op, Update) end,
			   Updates),
    #apbupdateobjects{updates = EncUpdates, transaction_descriptor = TxId};

encode(update_objects, {Updates, TxId, json}) ->
    EncUpdates = lists:map(fun(Update) ->
				   encode_json(update_op, Update) end,
			   Updates),
    JReq = [{update_objects, [EncUpdates, json_utilities:txid_to_json(TxId)]}],
    #apbjsonrequest{value=jsx:encode(JReq)};

encode(update_op, {Object={_Key, Type, _Bucket}, Op, Param}) ->
    EncObject = encode(bound_object, Object),
    case Type of
        riak_dt_pncounter -> EncUp = encode(counter_update, {Op, Param}),
                         #apbupdateop{boundobject=EncObject, optype = 1, counterop = EncUp};
        riak_dt_orset -> SetUp = encode(set_update, {Op, Param}),
                         #apbupdateop{boundobject=EncObject, optype = 2, setop=SetUp};
        crdt_pncounter -> EncUp = encode(counter_update, {Op, Param}),
                         #apbupdateop{boundobject=EncObject, optype = 1, counterop = EncUp};
        crdt_orset -> SetUp = encode(set_update, {Op, Param}),
                     #apbupdateop{boundobject=EncObject, optype = 2, setop=SetUp};
        riak_dt_gcounter -> EncUp = encode(counter_update, {Op, Param}),
                         #apbupdateop{boundobject=EncObject, optype = 1, counterop = EncUp};
        riak_dt_lwwreg -> EncUp = encode(reg_update, {Op, Param}),
                         #apbupdateop{boundobject=EncObject, optype = 3, regop = EncUp}

    end;

encode(static_update_objects, {Clock, Properties, Updates, proto_buf}) ->
    EncTransaction = encode(start_transaction, {Clock, Properties, proto_buf}),
    EncUpdates = lists:map(fun(Update) ->
                                  encode(update_op, Update) end,
                          Updates),
    #apbstaticupdateobjects{transaction = EncTransaction,
                            updates = EncUpdates};

encode(static_update_objects, {Clock, Properties, Updates, json}) ->
    EncTransaction = [{start_transaction, [vectorclock:to_json(Clock), encode_json(txn_properties, Properties)]}],
    EncUpdates = lists:map(fun(Update) ->
                                  encode_json(update_op, Update) end,
                          Updates),
    JReq = [{static_update_objects, [EncUpdates, EncTransaction]}],
    #apbjsonrequest{value=jsx:encode(JReq)};

encode(bound_object, {Key, Type, Bucket}) ->
    #apbboundobject{key=Key, type=encode(type,Type), bucket=Bucket};

encode(vectorclock, Clock) ->
    %% Fix this
    #apbvectorclock{value=term_to_binary(Clock)};

encode(type, riak_dt_pncounter) -> 0;
encode(type, riak_dt_gcounter) -> 1;
encode(type, riak_dt_orset) -> 2;
encode(type, crdt_pncounter) -> 3;
encode(type, crdt_orset) -> 4;
encode(type, riak_dt_lwwreg) -> 5;

encode(reg_update, {assign, Value}) ->
    #apbregupdate{optype = 1, value=Value};

encode(counter_update, {increment, Amount}) ->
    #apbcounterupdate{optype = 1, inc = Amount};

encode(counter_update, {decrement, Amount}) ->
    #apbcounterupdate{optype = 2, dec=Amount};

encode(set_update, {add, Elem}) ->
    #apbsetupdate{optype = 1, adds = [term_to_binary(Elem)]};
encode(set_update, {add_all, Elems}) ->
    BinElems = lists:map(fun(Elem) ->
                                term_to_binary(Elem)
                        end,
                        Elems),
    #apbsetupdate{optype = 2, adds = BinElems};
encode(set_update, {remove, Elem}) ->
    #apbsetupdate{optype = 3, rems = [term_to_binary(Elem)]};
encode(set_update, {remove_all, Elems}) ->
    BinElems = lists:map(fun(Elem) ->
                                term_to_binary(Elem)
                        end,
                        Elems),
    #apbsetupdate{optype = 4, rems = BinElems};

encode(read_objects, {Objects, TxId, proto_buf}) ->
    BoundObjects = lists:map(fun(Object) ->
                                     encode(bound_object, Object) end,
                             Objects),
    #apbreadobjects{boundobjects = BoundObjects, transaction_descriptor = TxId};

encode(read_objects, {Objects, TxId, json}) ->
    BoundObjects = lists:map(fun(Object) ->
                                     encode_json(bound_object, Object) end,
                             Objects),
    JReq = [{read_objects, [BoundObjects, json_utilities:txid_to_json(TxId)]}],
    #apbjsonrequest{value=jsx:encode(JReq)};

encode(static_read_objects, {Clock, Properties, Objects, proto_buf}) ->
    EncTransaction = encode(start_transaction, {Clock, Properties, proto_buf}),
    EncObjects = lists:map(fun(Object) ->
                                     encode(bound_object, Object) end,
                             Objects),
    #apbstaticreadobjects{transaction = EncTransaction,
                          objects = EncObjects};

encode(static_read_objects, {Clock, Properties, Objects, json}) ->
    EncTransaction = [{start_transaction, [vectorclock:to_json(Clock), encode_json(txn_properties, Properties)]}],
    EncObjects = lists:map(fun(O) ->
				   encode_json(bound_object, O) end,
			   Objects),
    JReq = [{static_update_objects, [EncObjects, EncTransaction]}],
    #apbjsonrequest{value=jsx:encode(JReq)};

encode(start_transaction_response, {error, Reason}) ->
    #apbstarttransactionresp{success=false, errorcode = encode(error_code, Reason)};

encode(start_transaction_response_json, {error, Reason}) ->
    #apbjsonresp{value=jsx:encode([{error, errorcode = encode(error_code, Reason)}])};

encode(start_transaction_response, {ok, TxId}) ->
    #apbstarttransactionresp{success=true, transaction_descriptor=term_to_binary(TxId)};

encode(start_transaction_response_json, {ok, TxId}) ->
    #apbjsonresp{value = jsx:encode([{ok, json_utilities:txid_to_json(TxId)}])};

encode(operation_response, {error, Reason}) ->
    #apboperationresp{success=false, errorcode = encode(error_code, Reason)};

encode(operation_response_json, {error, Reason}) ->
    #apbjsonresp{value=jsx:encode([{error, errorcode = encode(error_code, Reason)}])};

encode(operation_response, ok) ->
    #apboperationresp{success=true};

encode(operation_response_json, ok) ->
    #apbjsonresp{value=jsx:encode(ok)};

encode(commit_response, {error, Reason}) ->
    #apbcommitresp{success=false, errorcode = encode(error_code, Reason)};

encode(commit_response_json, {error, Reason}) ->
    #apbjsonresp{value=jsx:encode([{error, errorcode = encode(error_code, Reason)}])};

encode(commit_response, {ok, CommitTime}) ->
    #apbcommitresp{success=true, commit_time= term_to_binary(CommitTime)};

encode(commit_response_json, {ok, {DCID,CT}}) ->
    #apbjsonresp{value=jsx:encode([{commit_time,[json_utilities:dcid_to_json(DCID),CT]}])};

encode(read_objects_response, {error, Reason}) ->
    #apbreadobjectsresp{success=false, errorcode = encode(error_code, Reason)};

encode(read_objects_response_json, {error, Reason}) ->
    #apbjsonresp{value=jsx:encode([{error, errorcode = encode(error_code, Reason)}])};

encode(read_objects_response, {ok, Results}) ->
    EncResults = lists:map(fun(R) ->
                                   encode(read_object_resp, R) end,
                           Results),
    #apbreadobjectsresp{success=true, objects = EncResults};

encode(read_objects_response_json, {ok, Results}) ->
    EncResults = lists:map(fun(R) ->
                                   encode_json(read_object_resp, R) end,
                           Results),
    #apbjsonresp{value=jsx:encode([{success,EncResults}])};

encode(read_object_resp, {{_Key, riak_dt_lwwreg, _Bucket}, Val}) ->
    #apbreadobjectresp{reg=#apbgetregresp{value=term_to_binary(Val)}};

encode(read_object_resp, {{_Key, riak_dt_pncounter, _Bucket}, Val}) ->
    #apbreadobjectresp{counter=#apbgetcounterresp{value=Val}};

encode(read_object_resp, {{_Key, riak_dt_gcounter, _Bucket}, Val}) ->
    #apbreadobjectresp{counter=#apbgetcounterresp{value=Val}};

encode(read_object_resp, {{_Key, riak_dt_orset, _Bucket}, Val}) ->
    #apbreadobjectresp{set=#apbgetsetresp{value=term_to_binary(Val)}};

encode(read_object_resp, {{_Key, crdt_orset, _Bucket}, Val}) ->
    #apbreadobjectresp{set=#apbgetsetresp{value=term_to_binary(Val)}};

encode(static_read_objects_response, {error, Reason}) ->
    #apbcommitresp{success=false, errorcode = encode(error_code, Reason)};

encode(static_read_objects_response_json, {error, Reason}) ->
    #apbjsonresp{value=jsx:encode([{error, errorcode = encode(error_code, Reason)}])};

encode(static_read_objects_response, {ok, Results, CommitTime}) ->
    #apbstaticreadobjectsresp{
       objects = encode(read_objects_response, {ok, Results}),
       committime = encode(commit_response, {ok, CommitTime})};

encode(static_read_objects_response_json, {ok, Results, {DCID,CT}}) ->
    EncResults = lists:map(fun(R) ->
                                   encode_json(read_object_resp, R) end,
                           Results),
    EncCT = [{commit_time,[json_utilities:dcid_to_json(DCID),CT]}],
    #apbjsonresp{value=jsx:encode([{success,[EncResults,EncCT]}])};


%% For Legion clients

encode(get_objects, {Objects, ReplyType}) ->
    BoundObjects = lists:map(fun(Object) ->
				     case ReplyType of
					 profobuf ->
					     encode(bound_object, Object);
					 json ->
					     encode_json(bound_object, Object)
				     end
			     end, Objects),
    case ReplyType of
	proto_buf ->
	    #apbgetobjects{boundobjects = BoundObjects};
	json ->
	    JReq = [{get_objects,[[{bountobjects,BoundObjects}]]}],
	    #apbjsonrequest{value=jsx:encode(JReq)}
    end;

encode(get_objects_response, {error, Reason}) ->
    #apbgetobjectsresp{success=false, errorcode = encode(error_code, Reason)};

encode(get_objects_response_json, {error, Reason}) ->
    #apbjsonresp{value = jsx:encode([{error, encode(error_code, Reason)}])};
    %% #apbgetobjectsresp{success=false, errorcode = encode(error_code, Reason)};

encode(get_objects_response, {ok, Results}) ->
    EncResults = lists:map(fun(R) ->
                                   encode(get_object_resp, R) end,
                           Results),
    #apbgetobjectsresp{success=true, objects = EncResults};

encode(get_objects_response_json, {ok, Results}) ->
    EncResults = lists:map(fun(R) ->
                                   encode(get_object_resp_json, R) end,
                           Results),
    #apbjsonresp{value = jsx:encode([{success, [{get_objects_resp,EncResults}]}])};
    %% #apbgetobjectsresp{success=true, objects = jsx:encode(EncResults)};

encode(get_object_resp, {{_Key, _Type, _Bucket}, {Val,CommitTime}}) ->
    %%JsonVal = Type:to_json(Val),
    %%JsonClock = vectorclock:to_json(CommitTime),
    %%#apbobjectresp{value=jsx:encode([JsonVal,JsonClock])};
    #apbobjectresp{value=term_to_binary([Val,CommitTime])};

encode(get_object_resp_json, {{_Key, Type, _Bucket}, {Val,CommitTime}}) ->
    JsonVal = Type:to_json(Val),
    JsonClock = vectorclock:to_json(CommitTime),
    [{object_and_clock, [JsonVal,JsonClock]}];



encode(get_log_operations, {ObjectClockTuples,ReplyType}) ->
    {BoundObjects,Clocks} =
	lists:foldl(fun({Object,Clock},{AccObj,AccClock}) ->
			    case ReplyType of
				proto_buf ->
				    {AccObj++[encode(bound_object, Object)], AccClock++[encode(vectorclock, Clock)]};
				json ->
				    {AccObj++[encode_json(bound_object, Object)], AccClock++[encode_json(vectorclock, Clock)]}
			    end
		    end, {[],[]}, ObjectClockTuples),
    case ReplyType of
	proto_buf ->
	    #apbgetlogoperations{timestamps = Clocks, boundobjects = BoundObjects};
	json ->
	    JReq = [{get_log_operations,[[{timestamps,Clocks}],[{boundobjects,BoundObjects}]]}],
	    #apbjsonrequest{value=jsx:encode(JReq)}
    end;

encode(get_log_operations_response, {error, Reason}) ->
    #apbgetlogoperationsresp{success=false, errorcode = encode(error_code, Reason)};

encode(get_log_operations_response_json, {error, Reason}) ->
    #apbjsonresp{value = jsx:encode([{error, encode(error_code, Reason)}])};

encode(get_log_operations_response, {ok, Results}) ->
    EncResults = lists:map(fun(R) ->
                                   encode(get_log_operation_resp, R) end,
                           Results),
    #apbgetlogoperationsresp{success=true, objects = EncResults};

encode(get_log_operations_response_json, {ok, Results}) ->
    EncResults = lists:map(fun(R) ->
                                   encode(get_log_operation_resp_json, R) end,
                           Results),
    #apbjsonresp{value = jsx:encode([{success, [{get_log_operations_resp,EncResults}]}])};

encode(get_log_operation_resp, {{_Key, _Type, _Bucket}, Val}) ->
    %% TODO fix this to use proper protocol buffers
    Ops = 
	lists:map(fun(O) ->
			  {opid_and_payload,O}
		  end, Val),
    #apblogoperationresp{value=term_to_binary(Ops)};

%% Here should convert to json
encode(get_log_operation_resp_json, {{_Key, _Type, _Bucket}, Res}) ->
    JRes = 
	lists:map(fun({OpId,Payload}) ->
			  [{opid_and_payload,[OpId,json_utilities:clocksi_payload_to_json(Payload)]}]
		  end, Res),
    [{log_operations,JRes}];


encode(replytype_code, proto_buf) -> 0;
encode(replytype_code, json) -> 1;
encode(replytype_code, _Other) -> 1;

%% Add new error codes
encode(error_code, unknown) -> 0;
encode(error_code, timeout) -> 1;
encode(error_code, _Other) -> 0;

encode(_Other, _) ->
    erlang:error("Incorrect operation/Not yet implemented").

encode_json(txn_properties, Properties) ->
    [{txn_properties, Properties}];

encode_json(update_op, {Object, Op, Param}) ->
    [{update_op, [encode_json(bound_object,Object),Op,json_utilities:convert_to_json(Param)]}];

encode_json(bound_object, {Key, Type, Bucket}) ->
    [{bound_object, [Key, Type, Bucket]}];

encode_json(vectorclock, Clock) ->
    vectorclock:to_json(Clock);

encode_json(read_object_resp, {{_Key, Type, _Bucket}, Val}) ->
    [{object_type_and_val, [Type,json_utilities:convert_to_json(Val)]}].


decode(txn_properties, _Properties) ->
    {};
decode(bound_object, #apbboundobject{key = Key, type=Type, bucket=Bucket}) ->
    {Key, decode(type, Type), Bucket};

decode(type, 0) -> riak_dt_pncounter;
decode(type, 1) -> riak_dt_gcounter;
decode(type, 2) -> riak_dt_orset;
decode(type, 3) -> crdt_pncounter;
decode(type, 4) -> crdt_orset;
decode(type, 5) -> riak_dt_lwwreg;
decode(error_code, 0) -> unknown;
decode(error_code, 1) -> timeout;

decode(replytype_code, 0) -> proto_buf;
decode(replytype_code, 1) -> json;

decode(vectorclock, #apbvectorclock{value = BClock}) ->
    binary_to_term(BClock);

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
decode(reg_update, #apbregupdate{optype = _Op, value =Value}) ->
    {assign, Value};
decode(counter_update, #apbcounterupdate{optype = Op, inc = I, dec = D}) ->
    case Op of
        1 -> {increment, I};
        2 -> {decrement, D}
    end;

decode(set_update, #apbsetupdate{optype = Op, adds = A, rems = R}) ->
    case Op of
        1 ->
            case A of
                [Elem] -> {add, binary_to_term(Elem)}
            end;
        2 -> DecElem = lists:map(fun(Elem) ->
                                         binary_to_term(Elem)
                                 end,
                                 A),
             {add_all, DecElem};
        3-> case R of
                [Elem] -> {remove, binary_to_term(Elem)}
            end;
        4 -> DecElem = lists:map(fun(Elem) ->
                                         binary_to_term(Elem)
                                 end,
                                 R),
             {remove_all, DecElem}
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
    {set, erlang:binary_to_term(Val)};
decode_response(#apbreadobjectresp{reg = #apbgetregresp{value = Val}}) ->
    {reg, erlang:binary_to_term(Val)};
decode_response(#apbstaticreadobjectsresp{objects = Objects,
                                          committime = CommitTime}) ->
    {read_objects, Values} = decode_response(Objects),
    {commit_transaction, TimeStamp} = decode_response(CommitTime),
    {static_read_objects_resp, Values, TimeStamp};


%% For legion clients



decode_response(#apbjsonresp{value=Value}) ->
    case jsx:decode(Value,[{labels,atom}]) of
	[{success, Resp}] ->
	    decode_json(Resp);
	[{error, Reason}] ->
	    {error, decode(error_code, Reason)}
    end;
decode_response(#apbgetobjectsresp{success=false, errorcode=Reason}) ->
    {error, decode(error_code, Reason)};
decode_response(#apbgetobjectsresp{success=true, objects = Objects}) ->
    Resps = lists:map(fun(O) ->
                              decode_response(O) end,
                      Objects),
    {get_objects, Resps};
decode_response(#apbobjectresp{value = Val}) ->
    %% {object, jsx:decode(Val,[{labels,atom}])};
    {object, binary_to_term(Val)};
decode_response([{object_and_clock, [Object,Clock]}]) ->
    {object, [json_utilities:crdt_from_json(Object),vectorclock:from_json(Clock)]};


decode_response(#apbgetlogoperationsresp{success=false, errorcode=Reason}) ->
    {error, decode(error_code, Reason)};
decode_response(#apbgetlogoperationsresp{success=true, objects = Objects}) ->
    Resps = lists:map(fun(O) ->
                              decode_response(O) end,
                      Objects),
    {get_log_operations, Resps};
decode_response(#apblogoperationresp{value = Val}) ->
    {log_operations, erlang:binary_to_term(Val)};
decode_response([{opid_and_payload,[OpId,Payload]}]) ->
    io:format("opid and payload ~p", [[OpId,Payload]]),
    {opid_and_payload, [OpId,json_utilities:clocksi_payload_from_json(Payload)]};
decode_response(Other) ->
    erlang:error("Unexpected message: ~p",[Other]).


decode_json([{start_transaction, [JClock, JProperties]}]) ->
    Clock = vectorclock:vectorclock_from_json(JClock),
    Properties = decode_json(JProperties),
    {start_transaction, Clock, Properties, json};

decode_json([{abort_transaction, JTxId}]) ->
    TxId = json_utilites:txid_from_json(JTxId),
    {abort_transaction, TxId, json};

decode_json([{commit_transaction, JTxId}]) ->
    TxId = json_utilites:txid_from_json(JTxId),
    {commit_transaction, TxId, json};

decode_json([{update_objects, [JUpdates, JTxId]}]) ->
    TxId = json_utilities:txid_from_json(JTxId),
    Updates = lists:map(fun(JUp) ->
				decode_json(JUp)
			end, JUpdates),
    {update_objects, TxId, Updates, json};

decode_json([{static_update_objects, [JUpdates, JTransaction]}]) ->
    [{start_transaction, [[{timestamp,JClock}], JProperties]}] = JTransaction,
    Clock = vectorclock:vectorclock_from_json(JClock),
    Properties = decode_json(JProperties),
    Updates = lists:map(fun(JUp) ->
				decode_json(JUp)
			end, JUpdates),
    {static_update_objects, Clock, Updates, Properties, json};
    
decode_json([{get_objects,[[{bountobjects,JBoundObjects}]]}]) ->
    BoundObjects = lists:map(fun(O) ->
				     decode_json(O)
			     end, JBoundObjects),
    {get_objects,BoundObjects,json};
decode_json([{get_log_operations,[[{timestamps,JClocks}],[{boundobjects,JBoundObjects}]]}]) ->
    BoundObjects = lists:map(fun(O) ->
				     decode_json(O)
			     end, JBoundObjects),
    Clocks = lists:map(fun(O) ->
			       decode_json(O)
		       end, JClocks),
    {get_log_operations,BoundObjects,Clocks,json};

decode_json([{get_objects_resp, Objects}]) ->
    io:format("the json log resp: ~p", [Objects]),
    Resps =
	lists:map(fun(O) ->
			  decode_response(O) end,
		  Objects),
    {get_objects, Resps};
decode_json([{get_log_operations_resp, Objects}]) ->
    Resps =
	lists:map(fun([{log_operations, Ops}]) ->
			  lists:map(fun(O) ->
					    decode_response(O)
				    end, Ops)
		  end, Objects),
    {get_log_operations, Resps};

decode_json([{txn_properties,Properties}]) ->
    Properties;
decode_json([{update_op, [BoundObject,Op,Param]}]) ->
    {decode_json(BoundObject),Op,json_utilities:convert_from_json(Param)};


decode_json([{bound_object, [Key, Type, Bucket]}]) ->
    {Key,json_utilities:type_from_json(Type),Bucket};
decode_json([{vectorclock,Elements}]) ->
    vectorclock:from_json([{vectorclock,Elements}]);

decode_json([{object_type_and_val,[JType,JVal]}]) ->
    Type = json_utilities:type_from_json(JType),
    Val = json_utilities:deconvert_from_json(JVal),
    {Type,Val};

decode_json(Other) ->
    erlang:error("Unexpected json message: ~p",[Other]).



-ifdef(TEST).

%% Tests encode and decode
start_transaction_test() ->
    Clock = term_to_binary(ignore),
    Properties = {},
    EncRecord = antidote_pb_codec:encode(start_transaction,
                                         {Clock, Properties, proto_buf}),
    [MsgCode, MsgData] = riak_pb_codec:encode(EncRecord),
    Msg = riak_pb_codec:decode(MsgCode, list_to_binary(MsgData)),
    ?assertMatch(true, is_record(Msg,apbstarttransaction)),
    ?assertMatch(ignore, binary_to_term(Msg#apbstarttransaction.timestamp)),
    ?assertMatch(Properties,
                 antidote_pb_codec:decode(txn_properties,
                                          Msg#apbstarttransaction.properties)).

read_transaction_test() ->
    Objects = [{<<"key1">>, riak_dt_pncounter, <<"bucket1">>},
               {<<"key2">>, riak_dt_orset, <<"bucket2">>}],
    TxId = term_to_binary({12}),
         %% Dummy value, structure of TxId is opaque to client
    EncRecord = antidote_pb_codec:encode(read_objects, {Objects, TxId, proto_buf}),
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
    Updates = [ {{<<"1">>, riak_dt_pncounter, <<"2">>}, increment , 1},
                {{<<"2">>, riak_dt_gcounter, <<"2">>}, increment , 1},
                {{<<"a">>, riak_dt_orset, <<"2">>}, add , 3},
                {{<<"b">>, crdt_pncounter, <<"2">>}, increment , 2},
                {{<<"c">>, crdt_orset, <<"2">>}, add, 4},
                {{<<"a">>, riak_dt_orset, <<"2">>}, add_all , [5,6]}
              ],
    TxId = term_to_binary({12}),
         %% Dummy value, structure of TxId is opaque to client
    EncRecord = antidote_pb_codec:encode(update_objects, {Updates, TxId, proto_buf}),
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
