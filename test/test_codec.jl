using AzureSDK
const A = AzureSDK.AMQP
using Dates, UUIDs, Test

@testset "AMQP codec" begin
    @testset "primitive round trips" begin
        vals = Any[nothing, true, false, 0x01, 0x0102, UInt32(0), UInt32(5), UInt32(70000), UInt64(0), UInt64(200),
            typemax(UInt64), Int8(-3), Int16(-300), Int32(5), Int32(-70000), Int64(-1), typemax(Int64), typemin(Int64),
            1.5f0, 2.25, -0.0, Char(0x1F600), 'a', DateTime(2024, 1, 2, 3, 4, 5, 6), DateTime(1900, 1, 1), uuid4(),
            UInt8[1, 2, 3], UInt8[], rand(UInt8, 300), "hello", "", "x"^300, "ünïcødé ✓", :sym, Symbol("a"^300),
            Any[1, "a", Any[]], Dict{Any,Any}("a" => 1, :b => Any[1, 2], 3 => nothing), Any[rand(UInt8, 400), "y"]]
        for v in vals
            r = A.decode(A.encode(v))
            @test isequal(r, v)
        end
        @test isnan(A.decode(A.encode(NaN)))
    end

    @testset "compact encodings" begin
        @test A.encode(UInt32(0)) == [0x43]
        @test A.encode(UInt32(7)) == [0x52, 0x07]
        @test A.encode(UInt64(0)) == [0x44]
        @test A.encode(Int64(-2)) == [0x55, 0xfe]
        @test A.encode(true) == [0x41]
        @test A.encode(Any[]) == [0x45]
        @test A.encode("ab") == [0xa1, 0x02, 0x61, 0x62]
        @test A.encode(Any[UInt32(1)]) == [0xc0, 0x03, 0x01, 0x52, 0x01]
        big = A.encode(Any["x"^300])
        @test big[1] == 0xd0
    end

    @testset "arrays" begin
        for v in (UUID[uuid4(), uuid4()], Int64[1, -2, 3], [:a, :bb], ["x", "yy"], [DateTime(2020)], Bool[true, false],
            UInt32[1, 2], [rand(UInt8, 3), rand(UInt8, 300)])
            @test A.decode(A.encode(A.AMQPArray(v))) == v
        end
        @test A.decode(A.encode(A.AMQPArray(Int64[]))) == Int64[]
    end

    @testset "composites" begin
        o = A.Open(container_id="c1", hostname="h", max_frame_size=UInt32(65536),
            properties=Dict{Symbol,Any}(:product => "x"), desired_capabilities=[:foo])
        @test A.decode(A.encode(o)) == o
        t = A.Transfer(handle=UInt32(1), delivery_id=UInt32(7), delivery_tag=UInt8[1, 2], settled=false)
        @test A.decode(A.encode(t)) == t
        d = A.Disposition(role=true, first=UInt32(1), settled=true,
            state=A.Rejected(error=A.AMQPError(condition=Symbol("amqp:x"), description="bad")))
        @test A.decode(A.encode(d)) == d
        @test A.decode(A.encode(A.Accepted())) == A.Accepted()
        @test A.decode(A.encode(A.Modified(delivery_failed=true, undeliverable_here=true))) ==
              A.Modified(delivery_failed=true, undeliverable_here=true)
        filt = A.AMQPDescribed(Symbol("apache.org:selector-filter:string"), "x > 1")
        s = A.Source(address="q", filter=Dict{Any,Any}(Symbol("apache.org:selector-filter:string") => filt))
        @test A.decode(A.encode(s)) == s
        # "multiple" fields accept a single symbol
        list = Any["c", nothing, nothing, nothing, nothing, nothing, nothing, :one]
        @test A.from_list(A.Open, list).offered_capabilities == [:one]
        # mandatory fields
        @test_throws A.AMQPDecodeError A.from_list(A.Begin, Any[nothing])
        @test A.with_fields(o; hostname="z").hostname == "z"
    end

    @testset "messages" begin
        m = A.AMQPMessage(header=A.Header(durable=true, ttl=UInt32(1000)),
            delivery_annotations=Dict{Any,Any}(:x => 1),
            message_annotations=Dict{Any,Any}(Symbol("x-opt-partition-key") => "pk", "string-key" => 2),
            properties=A.Properties(message_id="m1", content_type=:json, creation_time=DateTime(2020), group_id="g"),
            application_properties=Dict{String,Any}("a" => 1, "b" => "s", "c" => 2.5, "d" => uuid4()),
            body_type=:data, body=[UInt8[1, 2, 3], UInt8[4]], footer=Dict{Any,Any}(:f => "x"))
        m2 = A.decode_message(A.encode_message(m))
        @test m2.header == m.header
        @test m2.properties == m.properties
        @test m2.application_properties == m.application_properties
        @test m2.body == m.body
        @test m2.message_annotations[Symbol("string-key")] == 2
        @test m2.footer == Dict{Any,Any}(:f => "x")
        @test A.body_bytes(m2) == UInt8[1, 2, 3, 4]
        v = A.decode_message(A.encode_message(A.AMQPMessage(body_type=:value, body=Dict{Any,Any}("k" => 1))))
        @test v.body_type === :value && v.body == Dict{Any,Any}("k" => 1)
        sq = A.decode_message(A.encode_message(A.AMQPMessage(body_type=:sequence, body=[Any[1, 2], Any["x"]])))
        @test sq.body_type === :sequence && sq.body == [Any[1, 2], Any["x"]]
    end

    @testset "frames" begin
        o = A.Open(container_id="c")
        f = A.encode_frame(o; channel=UInt16(3))
        fr = A.decode_frame(f[1:8], f[9:end])
        @test fr.body == o && fr.channel == 3 && fr.payload === nothing
        t = A.encode_frame(A.Transfer(handle=UInt32(0)); payload=UInt8[9, 9, 9])
        fr = A.decode_frame(t[1:8], t[9:end])
        @test collect(fr.payload) == UInt8[9, 9, 9]
        hb = A.decode_frame(A.HEARTBEAT_FRAME[1:8], UInt8[])
        @test hb.body === nothing
    end

    @testset "hostile input is rejected safely" begin
        bad = (UInt8[0xd0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],      # huge list count
            UInt8[0xc0, 0x05, 0xff],                                             # truncated
            UInt8[0xa1, 0x10, 0x41],                                             # string longer than data
            UInt8[0xf0, 0, 0, 0, 9, 0x7f, 0xff, 0xff, 0xff, 0x98],               # array count overflow
            UInt8[0xc1, 0x02, 0x01, 0x40],                                       # odd map
            UInt8[0xa1, 0x02, 0xff, 0xfe],                                       # invalid UTF-8
            UInt8[0x73, 0x00, 0x11, 0x00, 0x00],                                 # invalid char
            UInt8[0x56, 0x02],                                                   # invalid boolean
            UInt8[0x7f],                                                         # unknown constructor
            UInt8[])
        for b in bad
            @test_throws A.AMQPDecodeError A.decode(b)
        end
        nested = reduce((acc, _) -> Any[acc], 1:200; init=Any[])
        @test_throws A.AMQPDecodeError A.decode(A.encode(nested))
        @test_throws A.AMQPDecodeError A.decode_message(UInt8[0x00, 0x53, 0x75, 0x41])  # data section not binary
    end
end
