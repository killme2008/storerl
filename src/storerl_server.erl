%%%----------------------------------------------------------------------
%%%
%%% @copyright
%%%
%%% @author dennis <killme2008@gmail.com>
%%% @doc storerl server
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(storerl_server).
-author("killme2008@gmail.com").
-vsn('0.1').
-behaviour(gen_server).
-include("storerl.hrl").

-export([start/2,new/2]).
-export([test/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).
-define(OP_ADD,1).
-define(OP_DEL,2).
-define(I2L(X),integer_to_list(X)).
-define(L2B(X),list_to_binary(X)).
-record(opitem,{op,key,number,offset,length}).
-record(state,{dfs,lfs,df,lf,path,name,number,indices,dfcounter}).

test()->
Store=storerl_server:new("/home/dennis/programming/erlang","test"),
io:format("size:~p~n",[Store:size()]),

%io:format("~p~n",[Store:add(<<"key">>,<<"Value">>)]).
%Store:update(<<"dennis">>,"hello storerl"),
io:format("~p~n",[Store:get(<<"dennis">>)]),
io:format("~p~n",[Store:get(<<"catty">>)]),
io:format("~p~n",[Store:add(<<"dennis">>,<<"denniszhuang">>)]),
io:format("~p~n",[Store:add(<<"catty">>,<<"catty">>)]),
Store:close().

%io:format("~p~n",[Store:update(<<"key">>,<<"world">>)]),
%io:format("~p~n",[Store:get(<<"key">>)]),
%io:format("~p~n",[Store:erase(<<"key">>)]),
%io:format("~p~n",[Store:get(<<"key">>)]).

new(Path,Name)->
    {ok,Pid}=storerl_server:start(Path,Name),
    storerl:new(Pid).

start(Path,Name)->
    gen_server:start_link({local,?MODULE},?MODULE,[Path,Name],[]).



%%
%% gen_server callbacks
%%
init([Path,Name]) ->
    ok=filelib:ensure_dir(Path),
    DataFiles=dict:new(),
    LogFiles=dict:new(),
    Indices=dict:new(),
    DFCounter=dict:new(),

    {IndexList,{DataFiles2,LogFiles2,Indices2,DFCounter2}}=init_load(Path,Name,DataFiles,
                                                                     LogFiles,Indices,DFCounter),
    DFS=dict:size(DataFiles2),
    if  DFS > 0 ->
            Number=lists:max(IndexList),
            {ok,DataFile}=dict:find(Number,DataFiles2),
            {ok,LogFile}=dict:find(Number,LogFiles2),
            file:position(DataFile,eof),
            file:position(LogFile,eof),
            State=#state{path=Path,name=Name,df=DataFile,dfs=DataFiles2,lf=LogFile,lfs=LogFiles2,
                 number=Number,indices=Indices2,dfcounter=DFCounter2};
        true ->
            Number=0,
            DataFileName=filename:join([Path,Name ++ "." ++ ?I2L(Number)]),
            LogFileName=filename:join([Path,Name ++ "." ++ ?I2L(Number) ++ ".log"]),
            {ok,DataFile}=file:open(DataFileName,[raw,read,write]),
            {ok,LogFile}=file:open(LogFileName,[raw,read,write]),
            DataFiles3=dict:store(Number,DataFile,DataFiles2),
            LogFiles3=dict:store(Number,LogFile,LogFiles2),
            State=#state{path=Path,name=Name,df=DataFile,dfs=DataFiles3,lf=LogFile,lfs=LogFiles3,
                 number=Number,indices=Indices2,dfcounter=DFCounter2}
        end,
    {ok,State}.

handle_call(size, _From,State=#state{indices=Indices}) ->
    {reply,dict:size(Indices),State};

handle_call({add,Key,Value}, _From,State=#state{indices=Indices}) ->
    try dict:is_key(Key,Indices) of
       true ->
           {reply,{error,exists},State};
       false ->
          try
           {_OpItem,NewState}=inner_add(Key,Value,State),
           {reply,ok,NewState}
          catch
             _:Reason->
                  {error,Reason}
          end
    catch
        _:Reason->
           {error,Reason}
    end;

handle_call({update,Key,Value},_From,State=#state{indices=Indices}) ->
    try dict:find(Key,Indices) of
        {ok,OpItem} ->
           #opitem{number=Num,length=Len,offset=Offset}=OpItem,
           NewIndices=dict:erase(Key,Indices),
           {NewOpItem,NewState}=inner_add(Key,Value,State#state{indices=NewIndices}),
           #state{dfs=DataFiles,dfcounter=DFCounter,lfs=LogFiles,indices=NewNewIndices}=NewState,
           case OpItem#opitem.number =:= NewOpItem#opitem.number of
               true ->
                  {ok,OldDF}=dict:find(OpItem#opitem.number,DataFiles),
                  DFCounter2=dict:update_counter(OldDF,-1,DFCounter),
                  {reply,ok,NewState#state{dfcounter=DFCounter2}};
               false ->
                  {_,Indices2}=inner_remove(OpItem,DataFiles,LogFiles,NewNewIndices),
                  {reply,ok,NewState#state{indices=Indices2}}
           end;
        error ->
            {error,not_exists}
    catch
       _:Reason->
            {error,Reason}
    end;

handle_call({map,Fun}, _From,State=#state{dfs=DataFiles,indices=Indices}) ->
    Dict=dict:map(fun(Key,_)->
                          case inner_get(Key,DataFiles,Indices) of
                              {ok,Value}-> Fun(Key,Value);
                              Other ->  Other
                          end
                  end,Indices),
    {reply,{ok,Dict},State};

handle_call({get,Key}, _From,State=#state{dfs=DataFiles,indices=Indices}) ->
    Reply=inner_get(Key,DataFiles,Indices),
    {reply,Reply,State};
handle_call({delete,Key}, _From,State=#state{dfs=DataFiles,lfs=LogFiles,indices=Indices}) ->
    {Reply,NewIndices}=try dict:find(Key,Indices) of
        {ok,OpItem} ->
            inner_remove(OpItem,DataFiles,LogFiles,Indices);
        error ->
            {{error,not_exists},Indices}
    catch
       _:Reason->
            {{error,Reason},Indices}
    end,
    {reply,Reply,State#state{indices=NewIndices}}.

handle_cast(close,State) ->
    {stop,normal,State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State=#state{lfs=LogFiles,dfs=DataFiles}) ->
    dict:map(fun(_,File)->
                     file:close(File) end,LogFiles),
    dict:map(fun(_,File)->
                     file:close(File) end,DataFiles),
    ok.

code_change(_Old, State, _Extra) ->
    {ok, State}.

%%
%% internal API
%%
init_load(Path,Name,DataFiles,LogFiles,Indices,DFCounter)->
    NM=Name ++ ".",
    Indices=dict:new(),
    {ok,RE}=re:compile(Name ++ "\\.[0-9]+$"),
    {ok,Files}=file:list_dir(Path),
    IndexList=lists:sort(lists:map(fun(X)->
                                list_to_integer(string:substr(X,string:len(NM)+1))
                        end
               ,lists:filter(fun(Y)->
                                case re:run(Y,RE) of
                                     {match,_} ->
                                               true;
                                     _ ->
                                               false
                                end
                             end
                  ,Files))),
     {IndexList,read_log_file(IndexList,Path,Name,DataFiles,LogFiles,Indices,DFCounter)}.

read_log_file([],Path,Name,DataFiles,LogFiles,Indices,DFCounter) ->
    {DataFiles,LogFiles,Indices,DFCounter};
read_log_file([Idx|Tail],Path,Name,DataFiles,LogFiles,Indices,DFCounter)->
    {ok,DF}=file:open(filename:join(Path,Name ++"."++ ?I2L(Idx)),[raw,read,write]),
    {ok,LF}=file:open(filename:join(Path,Name ++"."++ ?I2L(Idx) ++ ".log"),[raw,read,write]),
    {Dict,NewIndices,NewDFCounter}=read_opitem(LF,DF,DataFiles,LogFiles,Indices,DFCounter),
    %io:format("~p~n~p~n",[dict:find(<<"key">>,Dict),dict:find(DF,NewDFCounter)]),
    read_log_file(Tail,Path,Name,dict:store(Idx,DF,DataFiles),dict:store(Idx,LF,LogFiles)
                  ,dict:merge(fun(K,V1,V2)-> V2 end,NewIndices,Dict),NewDFCounter).


read_opitem(LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter)->
     read_opitem(dict:new(),LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,file:read(LogFile,4)).
read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,eof)->
     {Dict,Indices,DFCounter};

read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,{ok,Bin})->
    read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,?L2B(Bin));
read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,<<TLen:32>>)->
    %io:format("~p~n",[TLen]),
    read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,file:read(LogFile,TLen));

read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,Data)->
    case parse_opitem(Data) of
        {error,_Reason}->
                   %%TODO log error
                    read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,
                                Indices,DFCounter,file:read(LogFile,4));

        OpItem ->
            #opitem{key=Key,op=OP}=OpItem,
            case OP of
                ?OP_ADD->
                    NewIndices=case dict:find(Key,Indices) of
                                   {ok,O}->
                                       {ok,Indices2}=inner_remove(O,DataFiles,LogFiles,Indices),
                                       Indices2;
                                   _ ->
                                       Indices
                               end,
                    {NewDict,NewDFCounter}=case dict:find(Key,Dict) of
                                               {ok,_} ->
                                                   {dict:update(Key,fun(_)-> OpItem end,Dict),DFCounter};
                                               _ ->
                                                   {dict:store(Key,OpItem,Dict),
                                                    dict:update_counter(DataFile,1,DFCounter)}
                                           end,
                    read_opitem(NewDict,LogFile,DataFile,DataFiles,LogFiles,
                                NewIndices,NewDFCounter,file:read(LogFile,4));

                 ?OP_DEL ->
                    NewDict=dict:erase(Key,Dict),
                    NewDFCounter=dict:update_counter(DataFile,-1,DFCounter),
                    read_opitem(NewDict,LogFile,DataFile,DataFiles,LogFiles,
                                Indices,NewDFCounter,file:read(LogFile,4));

                _ -> %% log unknow op
                    read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,
                                Indices,DFCounter,file:read(LogFile,4))


            end

    end.




inner_add(Key,Value,State=#state{df=DataFile,number=Number,lf=LogFile,indices=Indices,dfcounter=DFCounter})->
    {ok,Offset}=file:position(DataFile,cur),
    Len=get_len(Value),
    OpItem=#opitem{offset=Offset,key=Key,number=Number,op=?OP_ADD,length=Len},
    ok=file:write(DataFile,Value),
    ok=file:write(LogFile,gen_bin(OpItem)),
    Indices2=dict:store(Key,OpItem,Indices),
    DFCounter2=dict:update_counter(DataFile,1,DFCounter),
    {OpItem,State#state{indices=Indices2,dfcounter=DFCounter2}}.

gen_bin(OpItem=#opitem{offset=Offset,key=Key,number=Number,op=OP,length=Len})->
    KeyLen=get_len(Key),
    TLen=1+8+4+4+4+KeyLen,
    <<TLen:32,OP:8,Offset:64,Number:32,Len:32,KeyLen:32,Key:KeyLen/bytes>>.

parse_opitem(Bin= <<OP:8,Offset:64,Number:32,Len:32,KeyLen:32,Key:KeyLen/bytes>>)->
   #opitem{offset=Offset,key=Key,number=Number,op=OP,length=Len};
parse_opitem(_)->
    {error,log_file_error}.

inner_get(Key,DataFiles,Indices)->
    try dict:find(Key,Indices) of
        {ok,#opitem{number=Num,length=Len,offset=Offset}} ->
            try dict:find(Num,DataFiles) of
                {ok,DataFile} ->
                   file:pread(DataFile,Offset,Len);
                error ->
                    {error,data_file_removed}
            catch
               _:Reason->
                  {error,Reason}
            end;
        error ->
            {error,not_exists}
    catch
       _:Reason->
            {error,Reason}
    end.



inner_remove(OpItem=#opitem{number=Num,key=Key,length=Len,offset=Offset},DataFiles,LogFiles,Indices)->
    try dict:find(Num,DataFiles) of
        {ok,DataFile} ->
            try dict:find(Num,LogFiles) of
                {ok,LogFile}->
                    NewOpItem=#opitem{key=Key,length=Len,number=Num,offset=Offset,op=?OP_DEL},
                    ok=file:write(LogFile,gen_bin(NewOpItem)),
                    Indices2=dict:erase(Key,Indices),
                    {ok,Indices2};
                error->
                    {{error,log_file_removed},Indices}
            catch
                _:Reason->
                    {{error,Reason},Indices}
            end;
        error ->
            {{error,data_file_removed},Indices}
    catch
        _:Reason->
            {{error,Reason},Indices}
    end.

get_len(Data) when is_binary(Data)->
    size(Data);
get_len(Data) when is_list(Data) ->
    iolist_size(Data).

%%
%% EUNIT test
%%
-ifdef(EUNIT).
some_test() ->
    ?assert(true).
-endif.
