-module(storerl,[Store]).
-author('killme2008@gmail.com').
-export([add/2,update/2,get/1,erase/1,size/0,length/0,close/0,map/1]).

-type datal()  :: binary() | [char()].
%% @doc Add a key-value pair to storel,if Key already exists in storerl then return error.
-spec add(Key :: iodata(),Value :: iodata())->
                 'ok' | {'error',any()}.
add(Key,Value)->
    gen_server:call(Store,{add,Key,Value}).

%% @doc Update the a value in storerl to a new value,if Key is not present in storerl then return error.
-spec update(Key :: iodata(),Value :: iodata())->
                 'ok' | {'error',any()}.
update(Key,Value)->
    gen_server:call(Store,{update,Key,Value}).
%% @doc This function erase item with a given key from storel
-spec erase(Key :: iodata())->
                 'ok' | {'error',any()}.
erase(Key)->
    gen_server:call(Store,{delete,Key}).

%% @doc This function get a value associated with given key
-spec get(Key :: iodata())->
                 {'ok',DataL :: datal()} | {'error',any()}.
get(Key)->
    gen_server:call(Store,{get,Key}).
%% @doc Returns the number of elements in storerl.
-spec size() -> integer().
size()->
    gen_server:call(Store,size).
%% @doc Returns the number of elements in storerl.
-spec length() -> integer().
length()->
    size().
%% @doc map calls Func on successive keys and values of storerl to return a new value for each key. The evaluation order is undefined.
-spec map(fun((iodata(), iodata()) -> iodata())) -> dict().
map(Fun) when is_function(Fun)->
    gen_server:call(Store,{map,Fun}).

%% @doc Closes the storel,it must return ok.
-spec close()->
                   'ok'.
close()->
    gen_server:cast(Store,close).

