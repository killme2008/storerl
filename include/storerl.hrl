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
