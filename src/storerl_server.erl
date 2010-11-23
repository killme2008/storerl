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
%% initial a store instance
-export([start/2,start/3,new/3,new/2]).
%% gen_server callback functions
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-type store_opts() :: {'file_size', integer()} | {'max_file_count', integer()}.

%% @doc New a storerl instance
-spec new(string(), string()) -> tuple().
new(Path,Name)->
    new(Path,Name,[]).
%% @doc New a storerl instance
-spec new(string(), string(),[StoreOpts::store_opts()]) -> tuple().
new(Path,Name,Props)->
    {ok,Pid}=storerl_server:start(Path,Name,Props),
    storerl:new(Pid).

start(Path,Name)->
    start(Path,Name,[]).
start(Path,Name,Props)->
    gen_server:start_link({local,?MODULE},?MODULE,[Path,Name,Props],[]).



%%
%% gen_server callbacks
%%
init([Path,Name,Props]) ->
    ok=filelib:ensure_dir(Path),
    DataFiles=dict:new(),
    LogFiles=dict:new(),
    Indices=dict:new(),
    DFCounter=dict:new(),
    MaxFileCount=proplists:get_value(max_file_count,Props,?MAX_FILE_COUNT),
    FileSize=proplists:get_value(file_size,Props,?FILE_SIZE),
    {IndexList,{LoadedDataFiles,LoadedLogFiles,LoadedIndices,LoadedDFCounter}}=init_load(Path,Name,DataFiles,
                                                                     LogFiles,Indices,DFCounter),
    LoadedCount=dict:size(LoadedDataFiles),
    if  LoadedCount > 0 ->
            %%set current datafile and logfile
            Number=lists:max(IndexList),
            {ok,DataFile}=dict:find(Number,LoadedDataFiles),
            {ok,LogFile}=dict:find(Number,LoadedLogFiles),
            State=#state{path=Path,name=Name,df=DataFile,dfs=LoadedDataFiles,lf=LogFile,lfs=LoadedLogFiles,
                         max_file_count=MaxFileCount,file_size=FileSize,number=Number,indices=LoadedIndices,
                         dfcounter=LoadedDFCounter};
        true ->
            {Number,DataFile,LogFile}=new_file(-1,Path,Name),
            NewDataFiles=dict:store(Number,DataFile,LoadedDataFiles),
            NewLogFiles=dict:store(Number,LogFile,LoadedLogFiles),
            NewDFCounter=dict:store(DataFile,0,LoadedDFCounter),
            State=#state{path=Path,name=Name,df=DataFile,dfs=NewDataFiles,lf=LogFile,lfs=NewLogFiles,
                         number=Number,max_file_count=MaxFileCount,file_size=FileSize,
                         indices=LoadedIndices,dfcounter=NewDFCounter}
        end,
    {ok,State}.

handle_call(size, _From,State=#state{indices=Indices}) ->
    {reply,dict:size(Indices),State};

handle_call({add,Key,Value}, _From,State=#state{indices=Indices}) ->
    case dict:is_key(Key,Indices) of
       true ->
           {reply,{error,exists},State};
       false ->
          try
           {_,NewState}=inner_add(Key,Value,State),
           {reply,ok,NewState}
          catch
             Any:Reason->
                  {reply,{Any,Reason},State}
          end
    end;

handle_call({update,Key,Value},_From,State=#state{indices=Indices}) ->
    case dict:find(Key,Indices) of
        {ok,OpItem} ->
            try
                NewIndices=dict:erase(Key,Indices),
                {NewOpItem,NewState}=inner_add(Key,Value,State#state{indices=NewIndices}),
                #state{dfs=DataFiles,dfcounter=DFCounter}=NewState,
                case OpItem#opitem.number =:= NewOpItem#opitem.number of
                    true ->
                        %% update on same file
                        {ok,OldDF}=dict:find(OpItem#opitem.number,DataFiles),
                        NewDFCounter=dict:update_counter(OldDF,-1,DFCounter),
                        {reply,ok,NewState#state{dfcounter=NewDFCounter}};
                    false ->
                        %% update on different files,remove old opitem
                        {_,NewNewState}=inner_remove(OpItem,NewState),
                        {reply,ok,NewNewState}
                end
            catch
                Any:Reason ->
                    {reply,{Any,Reason},State}
                end;
        error ->
            {reply,{error,not_exists},State}
    end;

handle_call({map,Fun}, _From,State=#state{dfs=DataFiles,indices=Indices}) ->
    Dict= dict:map(fun(Key,_)->
                          case inner_get(Key,DataFiles,Indices) of
                              {ok,Value}->catch Fun(Key,Value);
                              Other ->  Other
                          end
                  end,Indices),
    {reply,{ok,Dict},State};

handle_call({get,Key}, _From,State=#state{dfs=DataFiles,indices=Indices}) ->
    Reply= inner_get(Key,DataFiles,Indices),
    {reply,Reply,State};
handle_call({delete,Key}, _From,State=#state{indices=Indices}) ->
    {Reply,NewState}=case dict:find(Key,Indices) of
        {ok,OpItem} ->
          try
              inner_remove(OpItem,State)
          catch
              Any:Reason->
                  {{Any,Reason},State}
          end;
        error ->
            {{error,not_exists},State}
    end,
    {reply,Reply,NewState}.

handle_cast(close,State) ->
    {stop,normal,State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State=#state{lfs=LogFiles,dfs=DataFiles}) ->
    %% close all opening datafiles and  logfiles
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

%%
%%@doc load existing logfile and datafile
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

read_log_file([],_Path,_Name,DataFiles,LogFiles,Indices,DFCounter) ->
    {DataFiles,LogFiles,Indices,DFCounter};
read_log_file([Idx|Tail],Path,Name,DataFiles,LogFiles,Indices,DFCounter)->
    {ok,DF}=file:open(filename:join(Path,Name ++"."++ ?I2L(Idx)),[raw,read,write]),
    {ok,LF}=file:open(filename:join(Path,Name ++"."++ ?I2L(Idx) ++ ".log"),[raw,read,write]),
    case read_opitem(LF,DF,DataFiles,LogFiles,Indices,DFCounter) of
        {ok,Dict,NewIndices,NewDFCounter} ->
            read_log_file(Tail,Path,Name,dict:store(Idx,DF,DataFiles),dict:store(Idx,LF,LogFiles)
                          ,dict:merge(fun(_K,_V1,V2)-> V2 end,NewIndices,Dict),NewDFCounter);
        {_error,_Reason} ->
            %%@todo log logfile error
            file:close(DF),
            file:close(LF),
            read_log_file(Tail,Path,Name,DataFiles,LogFiles,Indices,DFCounter)
    end.


read_opitem(LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter)->
     read_opitem(dict:new(),LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,file:read(LogFile,4)).

read_opitem(Dict,_LogFile,_DataFile,_DataFiles,_LogFiles,Indices,DFCounter,eof)->
     {ok,Dict,Indices,DFCounter};

read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,{ok,Bin})->
    read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,?L2B(Bin));

read_opitem(_Dict,_LogFile,_DataFile,_DataFiles,_LogFiles,_Indices,_DFCounter,{error,Reason})->
    {error,Reason};

read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,<<TLen:32>>) when is_integer(TLen) ->
    read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,file:read(LogFile,TLen));

read_opitem(Dict,LogFile,DataFile,DataFiles,LogFiles,Indices,DFCounter,Data)->
    case parse_opitem(Data) of
        {error,_Reason}->
            %%set postion to last correct position
            file:position(LogFile,{cur,0-(get_len(Data)+4)}),
            {ok,Dict,Indices,DFCounter};

        OpItem ->
            #opitem{key=Key,op=OP}=OpItem,
            case OP of
                ?OP_ADD->
                    %% if it was added before,but found it now,it must be that remove op was not written to logfile
                    %% we remove it now!~
                    {NewDFS,NewLFS,NewIndices,NewDFCounter}=case dict:find(Key,Indices) of
                                   {ok,ExistsOP}->
                                       {ok,NewState}=inner_remove(ExistsOP,#state{dfs=DataFiles,lfs=LogFiles,
                                                                           indices=Indices,dfcounter=DFCounter}),
                                       {NewState#state.dfs,NewState#state.lfs,NewState#state.indices,
                                        NewState#state.dfcounter};
                                   _ ->
                                       {DataFiles,LogFiles,Indices,DFCounter}
                               end,
                    %% add or update at same datafile,update counter
                    {NewDict,DFCounter2}=case dict:find(Key,Dict) of
                                               {ok,_} ->
                                                   {dict:update(Key,fun(_)-> OpItem end,Dict),NewDFCounter};
                                               _ ->
                                                   {dict:store(Key,OpItem,Dict),
                                                    dict:update_counter(DataFile,1,NewDFCounter)}
                                           end,
                    read_opitem(NewDict,LogFile,DataFile,NewDFS,NewLFS,
                                NewIndices,DFCounter2,file:read(LogFile,4));

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


inner_add(Key,Value,State=#state{df=DF,number=N,file_size=FileSize,path=Path,name=Name,lfs=LFS,dfs=DFS,
                                 lf=LF,indices=Indices,dfcounter=DFC})->
    {ok,Offset}=file:position(DF,cur),
    if
        %% if DataFile size is too big,then new datafile
        Offset >= FileSize ->
            {Number,DataFile,LogFile}=new_file(N,Path,Name),
            DataFiles=dict:store(Number,DataFile,DFS),
            LogFiles=dict:store(Number,LogFile,LFS),
            DFCounter=dict:store(DataFile,0,DFC),
            %% write to offset zero
            {OpItem,NewIndices,NewDFCounter}=inner_add(Key,Value,0,DataFile,Number,LogFile,Indices,DFCounter),
            {OpItem,State#state{df=DataFile,lf=LogFile,number=Number,dfs=DataFiles,
                                lfs=LogFiles,indices=NewIndices,dfcounter=NewDFCounter}};
        true ->
            {OpItem,NewIndices,NewDFCounter}=inner_add(Key,Value,Offset,DF,N,LF,Indices,DFC),
            {OpItem,State#state{indices=NewIndices,dfcounter=NewDFCounter}}
        end.

inner_add(Key,Value,Offset,DataFile,Number,LogFile,Indices,DFCounter)->
    Len=get_len(Value),
    OpItem=#opitem{offset=Offset,key=Key,number=Number,op=?OP_ADD,length=Len},
    ok=file:write(DataFile,Value),
    ok=file:write(LogFile,gen_bin(OpItem)),
    NewIndices=dict:store(Key,OpItem,Indices),
    NewDFCounter=dict:update_counter(DataFile,1,DFCounter),
    {OpItem,NewIndices,NewDFCounter}.

%% generate binary from opitem
gen_bin(#opitem{offset=Offset,key=Key,number=Number,op=OP,length=Len})->
    KeyLen=get_len(Key),
    TLen=1+8+4+4+4+KeyLen,
    <<TLen:32,OP:8,Offset:64,Number:32,Len:32,KeyLen:32,Key:KeyLen/bytes>>.
%% parse opitem from binary
parse_opitem(_Bin= <<OP:8,Offset:64,Number:32,Len:32,KeyLen:32,Key:KeyLen/bytes>>)->
   #opitem{offset=Offset,key=Key,number=Number,op=OP,length=Len};
parse_opitem(_)->
    {error,log_file_error}.

%% new datafile
new_file(Number,Path,Name)->
    NewNum=Number+1,
    DataFileName=filename:join([Path,Name ++ "." ++ ?I2L(NewNum)]),
    LogFileName=filename:join([Path,Name ++ "." ++ ?I2L(NewNum) ++ ".log"]),
    {ok,DataFile}=file:open(DataFileName,[raw,read,write]),
    {ok,LogFile}=file:open(LogFileName,[raw,read,write]),
    {NewNum,DataFile,LogFile}.

inner_get(Key,DataFiles,Indices)->
    case dict:find(Key,Indices) of
        {ok,#opitem{number=Num,length=Len,offset=Offset}} ->
            case dict:find(Num,DataFiles) of
                {ok,DataFile} ->
                   file:pread(DataFile,Offset,Len);
                error ->
                    {error,data_file_removed}
            end;
        error ->
            {error,not_exists}
    end.


inner_remove(_OpItem=#opitem{number=Num,key=Key,length=Len,offset=Offset},
             State=#state{dfs=DataFiles,lfs=LogFiles,number=Number,indices=Indices,
                          dfcounter=DFCounter,df=CurDF,name=Name,path=Path,file_size=FileSize})->
    case dict:find(Num,DataFiles) of
        {ok,DataFile} ->
            case dict:find(Num,LogFiles) of
                {ok,LogFile}->
                    DelOpItem=#opitem{key=Key,length=Len,number=Num,offset=Offset,op=?OP_DEL},
                    ok=file:write(LogFile,gen_bin(DelOpItem)),
                    NewIndices=dict:erase(Key,Indices),
                    {ok,Count}=dict:find(DataFile,DFCounter),
                    {ok,Size}=file:position(DataFile,cur),
                    if
                        %% There is no reference associated with  Datafile and file size is greater than FileSiz
                        %% delete DataFile and counter
                         (Count =:= 1) and (Size >= FileSize) ->
                            NewDFS=dict:erase(Num,DataFiles),
                            NewLFS=dict:erase(Num,LogFiles),
                            NewDFCounter=dict:erase(DataFile,DFCounter),
                            if
                                %% if DataFile is current datafile,new datafile
                                DataFile =:= CurDF ->
                                    delete_file(DataFile,LogFile,Path,Name,Num),
                                    {NewNumber,NewDataFile,NewLogFile}=new_file(Number,Path,Name),
                                    NewNewDFS=dict:store(NewNumber,NewDataFile,NewDFS),
                                    NewNewLFS=dict:store(NewNumber,NewLogFile,NewLFS),
                                    NewNewDFCounter=dict:store(NewDataFile,0,NewDFCounter),
                                    {ok,State#state{df=NewDataFile,lf=NewLogFile,number=NewNumber,
                                                    dfs=NewNewDFS,lfs=NewNewLFS,dfcounter=NewNewDFCounter,
                                                    indices=NewIndices}};
                                true  ->
                                    delete_file(DataFile,LogFile,Path,Name,Num),
                                    {ok,State#state{dfs=NewDFS,lfs=NewLFS,dfcounter=NewDFCounter,
                                                    indices=NewIndices}}

                            end;

                        true ->
                            NewDFCounter=dict:update_counter(DataFile,-1,DFCounter),
                            {ok,State#state{indices=NewIndices,dfcounter=NewDFCounter}}

                    end;
                error->
                    {{error,log_file_removed},State}
            end;
        error ->
            {{error,data_file_removed},State}
    end.

delete_file(DataFile,LogFile,Path,Name,Number)->
    ok=file:close(DataFile),
    ok=file:close(LogFile),
    ok=file:delete(filename:join(Path,Name ++ "." ++ ?I2L(Number))),
    ok=file:delete(filename:join(Path,Name ++ "." ++ ?I2L(Number) ++ ".log")).

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

