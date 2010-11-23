-module(benchmark).
-export([test/0]).

test()->

Store=storerl_server:new("/home/dennis/programming/erlang","test"),

Store:add(<<"hello">>,"test"),
lists:foreach(fun(N)->
                      Bin=list_to_binary(integer_to_list(N)),
                      Store:add(Bin,Bin),
                      Store:update(Bin,Bin),
                      Store:erase(Bin)
                      end,
   lists:duplicate(10000000,1000000000)),
io:format("~p~n",[Store:get(<<"hello">>)]),
Store:map(fun(K,V)->
                  io:format("~p=~p~n",[K,V])
                      end),
io:format("size:~p~n",[Store:size()]),
Store:erase(<<"hello">>),
io:format("~p~n",[Store:get(<<"hello">>)]),
Store:close().
