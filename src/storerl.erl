-module(storerl,[Store]).
-author('killme2008@gmail.com').
-export([add/2,update/2,get/1,erase/1,size/0,length/0,close/0,map/1]).

add(Key,Value)->
    gen_server:call(Store,{add,Key,Value}).

update(Key,Value)->
    gen_server:call(Store,{update,Key,Value}).

erase(Key)->
    gen_server:call(Store,{delete,Key}).

get(Key)->
    gen_server:call(Store,{get,Key}).

size()->
    gen_server:call(Store,size).

length()->
    size().
map(Fun) when is_function(Fun)->
    gen_server:call(Store,{map,Fun}).
close()->
    gen_server:cast(Store,close).

