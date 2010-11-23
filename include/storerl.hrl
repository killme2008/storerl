%%%----------------------------------------------------------------------
%%%
%%% @copyright
%%%
%%% @author killme2008@gmail.com
%%% @doc storerl header file
%%%
%%%----------------------------------------------------------------------
-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(WARN2(F, D), io:format(F, D)).
-define(INFO2(F, D), io:format(F, D)).
-define(ERROR2(F, D), io:format(F, D)).
-define(FATAL2(F, D), io:format(F, D)).
-define(DEBUG2(F, D), io:format(F, D)).
%%% op codes
-define(OP_ADD,1).
-define(OP_DEL,2).
%% Max file size per datafile,default 64m.
-define(FILE_SIZE,67108864).
%% Max datafile count,default 1024*1024*1024
-define(MAX_FILE_COUNT,1073741824).
-define(I2L(X),integer_to_list(X)).
-define(L2B(X),list_to_binary(X)).

%%Server state
-record(opitem,{op,key,number,offset,length}).
-record(state,{dfs,lfs,df,lf,path,name,number,max_file_count,
               file_size,indices,dfcounter}).
-define(MODES,[read,write,raw,delayed_write]).
-define(FNAME_PATTERN,".*\\.(\\d+)$").
