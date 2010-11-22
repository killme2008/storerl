%%%----------------------------------------------------------------------
%%%
%%% @copyright 
%%%
%%% @author dennis <killme2008@gmail.com>
%%% @doc storerl app and supervisor callback
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(storerl_app).
-author("killme2008@gmail.com").
-vsn('0.1').
-include("storerl.hrl").

-behaviour(application).
-behaviour(supervisor).

-export([start/0]).
-export([start/2, stop/1]).
-export([init/1]).


%% @doc start the application from the erl shell
-spec start() -> 'ok' | {'error', any()}.
start() ->
    ensure_apps(),
    ?DEBUG2("start the ~p application~n", [?MODULE]),
    storerl_ctl:init(),
    application:start(storerl).

%% @doc the application start callback
-spec start(Type :: any(), Args :: any()) -> any().
start(_Type, _Args) ->
    ?DEBUG2("start the supervisor sup ~n", []),
    supervisor:start_link({local, storerl_sup}, ?MODULE, []).

%% @doc the application  stop callback
stop(_State) ->
    ok.

%% @doc supervisor callback
init(_Args) -> 
    ?DEBUG2("init supervisor~n", []),
    
    Stragegy = {one_for_one, 10, 10},

    ModServer = {storerl_server, {storerl_server, start_link, []},
                permanent, 2000, worker, [storerl_server]},

    {ok, {Stragegy, [
                    ModServer 
                    ]}
    }.

%%
%% internal API
%%

%% first ensure some apps must start
ensure_apps() ->
    application:start(sasl),
    ok.
