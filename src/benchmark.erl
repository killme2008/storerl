-module(benchmark).
-export([test_storerl/0,test_dets/0]).

-define(MESSAGE_LEN_LIST,[64,256,512,1024,4096]).
-define(REPEAT,100000).
-define(KEY,<<"Key">>).

test_dets()->
    benchmark("dets",fun(Name)->
                                dets:open_file(Name,[{type,set}])
                        end,
              fun(_,Name,Value)->
                      ok=dets:insert(Name,{?KEY,Value}),
                      dets:lookup(Name,?KEY),
                      ok=dets:delete(Name,?KEY)
              end,
             fun(Name,_)-> dets:close(Name) end).



test_storerl()->
    benchmark("storerl",fun(Name)->
                                storerl_server:new("/home/dennis/programming/erlang/storerl",atom_to_list(Name))
                        end,
              fun(Store,_,Value)->
                      ok=Store:add(?KEY,Value),
                      Store:get(?KEY),
                      ok=Store:erase(?KEY)
              end,
             fun(_,Store)->
                     Store:close()
             end).

benchmark(Type,InitFun,TestFun,Destroy)->
    lists:foreach(fun(Len)->
                          Name=list_to_atom("test_" ++ integer_to_list(Len)),
                          State=InitFun(Name),
                          Value=list_to_binary(lists:duplicate(Len,"a")),
                          statistics(wall_clock),
                          lists:foreach(fun(_)->
                                                TestFun(State,Name,Value)
                                        end,
                                        lists:duplicate(?REPEAT,dummy)),
                          {_,T1}=statistics(wall_clock),
                          Destroy(Name,State),
                          io:format("~p ~p message time:~p~n",[Type,Len,T1])
                  end,?MESSAGE_LEN_LIST).



