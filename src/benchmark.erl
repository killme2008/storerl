-module(benchmark).
-export([test/0,test_dets/0]).

-define(MESSAGE_LEN_LIST,[64,256,512,1024,4096]).
-define(REPEAT,100000).
-define(KEY,<<"Key">>).

test_dets()->
    statistics(wall_clock),
    lists:foreach(fun(Len)->
                          Name=list_to_atom("test_" ++ integer_to_list(Len)),
                          dets:open_file(Name,[{type,set}]),
                          Value=list_to_binary(lists:duplicate(Len,"a")),
                          statistics(wall_clock),
                          lists:foreach(fun(_)->
                                                ok=dets:insert(Name,{?KEY,Value}),
                                                dets:lookup(Name,?KEY),
                                                ok=dets:delete(Name,?KEY)
                                        end,
                                        lists:duplicate(?REPEAT,dummy)),
                          {_,T1}=statistics(wall_clock),
                          io:format("~p message time:~p~n",[Len,T1])
                  end,?MESSAGE_LEN_LIST).




test()->
    statistics(wall_clock),
    lists:foreach(fun(Len)->
                          Name=list_to_atom("test_" ++ integer_to_list(Len)),
                          Store=storerl_server:new("/home/dennis/programming/erlang/storerl","test"),
                          Value=list_to_binary(lists:duplicate(Len,"a")),
                          statistics(wall_clock),
                          lists:foreach(fun(_)->
                                                ok=Store:add(?KEY,Value),
                                                Store:get(?KEY),
                                                ok=Store:erase(?KEY)
                                        end,
                                        lists:duplicate(?REPEAT,dummy)),
                          {_,T1}=statistics(wall_clock),
                          Store:close(),
                          io:format("~p message time:~p~n",[Len,T1])
                  end,?MESSAGE_LEN_LIST).




