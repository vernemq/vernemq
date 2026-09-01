%% Copyright 2018-2026 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_proxy_protocol).

-export([recv_proxy_header/2, recv_proxy_header/3, check_trusted_proxy/2, parse/1]).

-spec recv_proxy_header(any(), timeout()) ->
    {ok, map()} | {error, closed | atom()} | {error, protocol_error, atom()}.
recv_proxy_header(Socket, Timeout) ->
    recv_proxy_header_tcp(Socket, Timeout).

-spec recv_proxy_header(ranch:ref(), timeout(), undefined | string()) ->
    {ok, map()} | {error, closed | atom()} | {error, protocol_error, atom()}.
recv_proxy_header(Ref, Timeout, TrustedProxy) ->
    receive
        HandshakeState = {handshake, Ref, ranch_ssl, SSLSocket, _} ->
            self() ! HandshakeState,
            TCPSocket = tcp_socket(SSLSocket),
            case check_trusted_proxy(TCPSocket, TrustedProxy) of
                ok -> recv_proxy_header_tcp(TCPSocket, Timeout);
                Error -> Error
            end
    end.

-spec check_trusted_proxy(any(), undefined | string()) -> ok | {error, any()}.
check_trusted_proxy(_, undefined) ->
    ok;
check_trusted_proxy(_, "") ->
    ok;
check_trusted_proxy(Socket, TrustedProxy) ->
    case inet:peername(Socket) of
        {ok, {IP, _Port}} ->
            TrustedProxies = string:tokens(TrustedProxy, ";"),
            case lists:member(inet:ntoa(IP), TrustedProxies) of
                true -> ok;
                false -> {error, proxy_protocol_trusted_proxy_not_accepted}
            end;
        {error, _} = Error ->
            Error
    end.

recv_proxy_header_tcp(Socket, Timeout) ->
    case gen_tcp:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            case parse(Data) of
                {ok, ProxyInfo, <<>>} ->
                    {ok, ProxyInfo};
                {ok, ProxyInfo, Rest} ->
                    case gen_tcp:unrecv(Socket, Rest) of
                        ok -> {ok, ProxyInfo};
                        Error -> Error
                    end;
                {error, HumanReadable} ->
                    {error, protocol_error, HumanReadable}
            end;
        Error ->
            Error
    end.

tcp_socket(SSLSocket) ->
    case element(2, SSLSocket) of
        {gen_tcp, TCPSocket, _, _} -> TCPSocket;
        TCPSocket -> TCPSocket
    end.

-spec parse(binary()) -> {ok, map(), binary()} | {error, atom()}.
parse(<<"\r\n\r\n\0\r\nQUIT\n", Rest/bits>>) ->
    parse_v2(Rest);
parse(Data) ->
    ranch_proxy_header:parse(Data).

parse_v2(<<2:4, 0:4, _:8, Len:16, Rest0/bits>>) ->
    case Rest0 of
        <<_:Len/binary, Rest/bits>> ->
            {ok, #{version => 2, command => local}, Rest};
        _ ->
            {error, 'Missing data in the PROXY protocol binary header. (PP 2.2)'}
    end;
parse_v2(<<2:4, 1:4, Family:4, Protocol:4, Len:16, Rest/bits>>) when
    Family =< 3, Protocol =< 2
->
    case Rest of
        <<Header:Len/binary, _/bits>> ->
            parse_v2(
                Rest,
                Len,
                parse_family(Family),
                parse_protocol(Protocol),
                <<Family:4, Protocol:4, Len:16, Header:Len/binary>>
            );
        _ ->
            {error, 'Missing data in the PROXY protocol binary header. (PP 2.2)'}
    end;
parse_v2(<<Version:4, _/bits>>) when Version =/= 2 ->
    {error, 'Invalid version in the PROXY protocol binary header. (PP 2.2)'};
parse_v2(<<_:4, Command:4, _/bits>>) when Command > 1 ->
    {error, 'Invalid command in the PROXY protocol binary header. (PP 2.2)'};
parse_v2(<<_:8, Family:4, _/bits>>) when Family > 3 ->
    {error, 'Invalid address family in the PROXY protocol binary header. (PP 2.2)'};
parse_v2(<<_:12, Protocol:4, _/bits>>) when Protocol > 2 ->
    {error, 'Invalid transport protocol in the PROXY protocol binary header. (PP 2.2)'};
parse_v2(_) ->
    {error, 'Missing data in the PROXY protocol binary header. (PP 2.2)'}.

parse_family(0) -> undefined;
parse_family(1) -> ipv4;
parse_family(2) -> ipv6;
parse_family(3) -> unix.

parse_protocol(0) -> undefined;
parse_protocol(1) -> stream;
parse_protocol(2) -> dgram.

parse_v2(Data, Len, Family, Protocol, _) when Family =:= undefined; Protocol =:= undefined ->
    <<_:Len/binary, Rest/bits>> = Data,
    {ok,
        #{
            version => 2,
            command => proxy,
            transport_family => undefined,
            transport_protocol => undefined
        },
        Rest};
parse_v2(
    <<S1, S2, S3, S4, D1, D2, D3, D4, SrcPort:16, DestPort:16, Rest/bits>>,
    Len,
    Family = ipv4,
    Protocol,
    Header
) when Len >= 12 ->
    parse_tlv(
        Rest,
        Len - 12,
        #{
            version => 2,
            command => proxy,
            transport_family => Family,
            transport_protocol => Protocol,
            src_address => {S1, S2, S3, S4},
            src_port => SrcPort,
            dest_address => {D1, D2, D3, D4},
            dest_port => DestPort
        },
        Header
    );
parse_v2(
    <<
        S1:16,
        S2:16,
        S3:16,
        S4:16,
        S5:16,
        S6:16,
        S7:16,
        S8:16,
        D1:16,
        D2:16,
        D3:16,
        D4:16,
        D5:16,
        D6:16,
        D7:16,
        D8:16,
        SrcPort:16,
        DestPort:16,
        Rest/bits
    >>,
    Len,
    Family = ipv6,
    Protocol,
    Header
) when Len >= 36 ->
    parse_tlv(
        Rest,
        Len - 36,
        #{
            version => 2,
            command => proxy,
            transport_family => Family,
            transport_protocol => Protocol,
            src_address => {S1, S2, S3, S4, S5, S6, S7, S8},
            src_port => SrcPort,
            dest_address => {D1, D2, D3, D4, D5, D6, D7, D8},
            dest_port => DestPort
        },
        Header
    );
parse_v2(
    <<SrcAddr0:108/binary, DestAddr0:108/binary, Rest/bits>>, Len, Family = unix, Protocol, Header
) when
    Len >= 216
->
    try
        [SrcAddr, _] = binary:split(SrcAddr0, <<0>>),
        true = byte_size(SrcAddr) > 0,
        [DestAddr, _] = binary:split(DestAddr0, <<0>>),
        true = byte_size(DestAddr) > 0,
        parse_tlv(
            Rest,
            Len - 216,
            #{
                version => 2,
                command => proxy,
                transport_family => Family,
                transport_protocol => Protocol,
                src_address => SrcAddr,
                dest_address => DestAddr
            },
            Header
        )
    catch
        _:_ ->
            {error, 'Invalid UNIX address in PROXY protocol binary header. (PP 2.2)'}
    end;
parse_v2(_, _, _, _, _) ->
    {error, 'Invalid length in the PROXY protocol binary header. (PP 2.2)'}.

parse_tlv(Rest, 0, Info, _) ->
    {ok, Info, Rest};
parse_tlv(<<16#1, TLVLen:16, ALPN:TLVLen/binary, Rest/bits>>, Len, Info, Header) ->
    parse_tlv(Rest, Len - TLVLen - 3, Info#{alpn => ALPN}, Header);
parse_tlv(<<16#2, TLVLen:16, Authority:TLVLen/binary, Rest/bits>>, Len, Info, Header) ->
    parse_tlv(Rest, Len - TLVLen - 3, Info#{authority => Authority}, Header);
parse_tlv(<<16#3, TLVLen:16, CRC32C:32, Rest/bits>>, Len0, Info, Header) when
    TLVLen =:= 4, Len0 >= 7
->
    Len = Len0 - TLVLen - 3,
    BeforeLen = byte_size(Header) - Len - TLVLen,
    <<Before:BeforeLen/binary, _:32, After:Len/binary>> = Header,
    case ranch_crc32c:crc32c(2900412422, [Before, <<0:32>>, After]) of
        CRC32C -> parse_tlv(Rest, Len, Info, Header);
        _ -> {error, 'Failed CRC32C verification in PROXY protocol binary header. (PP 2.2)'}
    end;
parse_tlv(<<16#3, _/bits>>, _, _, _) ->
    {error, 'Invalid TLV length in the PROXY protocol binary header. (PP 2.2)'};
parse_tlv(<<16#4, TLVLen:16, _:TLVLen/binary, Rest/bits>>, Len, Info, Header) ->
    parse_tlv(Rest, Len - TLVLen - 3, Info, Header);
parse_tlv(<<16#5, TLVLen:16, UniqueID:TLVLen/binary, Rest/bits>>, Len, Info, Header) when
    TLVLen =< 128
->
    parse_tlv(Rest, Len - TLVLen - 3, Info#{unique_id => UniqueID}, Header);
parse_tlv(<<16#5, _/bits>>, _, _, _) ->
    {error, 'Invalid TLV length in the PROXY protocol binary header. (PP 2.2, PP 2.2.5)'};
parse_tlv(<<16#20, TLVLen:16, Client, Verify:32, Rest0/bits>>, Len, Info, Header) ->
    SubsLen = TLVLen - 5,
    case Rest0 of
        <<Subs:SubsLen/binary, Rest/bits>> ->
            SSL0 = #{client => parse_client(<<Client>>), verified => Verify =:= 0},
            case parse_ssl_tlv(Subs, SubsLen, SSL0) of
                {ok, SSL, <<>>} ->
                    parse_tlv(Rest, Len - TLVLen - 3, Info#{ssl => SSL}, Header);
                Error = {error, _} ->
                    Error
            end;
        _ ->
            {error, 'Invalid TLV length in the PROXY protocol binary header. (PP 2.2)'}
    end;
parse_tlv(<<16#30, TLVLen:16, NetNS:TLVLen/binary, Rest/bits>>, Len, Info, Header) ->
    parse_tlv(Rest, Len - TLVLen - 3, Info#{netns => NetNS}, Header);
parse_tlv(<<TLVType, TLVLen:16, TLVValue:TLVLen/binary, Rest/bits>>, Len, Info, Header) ->
    RawTLVs = maps:get(raw_tlvs, Info, []),
    parse_tlv(Rest, Len - TLVLen - 3, Info#{raw_tlvs => [{TLVType, TLVValue} | RawTLVs]}, Header);
parse_tlv(_, _, _, _) ->
    {error, 'Invalid TLV length in the PROXY protocol binary header. (PP 2.2)'}.

parse_client(<<_:5, ClientCertSess:1, ClientCertConn:1, ClientSSL:1>>) ->
    Client0 =
        case ClientCertSess of
            0 -> [];
            1 -> [cert_sess]
        end,
    Client1 =
        case ClientCertConn of
            0 -> Client0;
            1 -> [cert_conn | Client0]
        end,
    case ClientSSL of
        0 -> Client1;
        1 -> [ssl | Client1]
    end.

parse_ssl_tlv(Rest, 0, Info) ->
    {ok, Info, Rest};
parse_ssl_tlv(<<TLVType, TLVLen:16, TLVValue:TLVLen/binary, Rest/bits>>, Len, Info) ->
    case ssl_subtype(TLVType) of
        undefined ->
            RawTLVs = maps:get(raw_tlvs, Info, []),
            parse_ssl_tlv(Rest, Len - TLVLen - 3, Info#{raw_tlvs => [{TLVType, TLVValue} | RawTLVs]});
        Type ->
            parse_ssl_tlv(Rest, Len - TLVLen - 3, Info#{Type => TLVValue})
    end;
parse_ssl_tlv(_, _, _) ->
    {error, 'Invalid TLV length in the PROXY protocol binary header. (PP 2.2)'}.

ssl_subtype(16#21) -> version;
ssl_subtype(16#22) -> cn;
ssl_subtype(16#23) -> cipher;
ssl_subtype(16#24) -> sig_alg;
ssl_subtype(16#25) -> key_alg;
ssl_subtype(16#26) -> group;
ssl_subtype(16#27) -> sig_scheme;
ssl_subtype(16#28) -> client_cert;
ssl_subtype(_) -> undefined.
