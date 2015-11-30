
module Worker
using Manager: StartWork, Ping
using Cairn.Client

immutable t
    port::Int64
end



function Handle(job::MapJob, conn::TCPSocket)
    mktempdir() do d
        master = Cairn.Client.find_connection(job.dfs)
        exe_f, _ = mktemp(d)
        close(_)
        Cairn.Client.download(master, [job.executable], d, [exe_f])
        UDF = evalfile(map_f) #TODO Note, horrbily insecure! Also TODO: Implement non-jl files


        f_ch = Channel{ASCIIString}(3)
        location = mktempdir(d)
        @async Cairn.Client.download(master, job.files, location, Nullable(f_ch))

        mktempdir() do  results
            count = length(job.files)
            # Run UDF in sync
            while count != 0
                count -= 1
                orig, f = take!(f_ch)
                UDF(orig, f, results)
            end

            # Do Shuffle
            @sync for name = readdir(results)
                @async begin
                    object = "$(job.results_dir).$name"
                    of = joinpath(results, name)
                    r = Cairn.command_result(master, Cairn.RPC.CreateRequest(object, job.replication))::Cairn.RPC.CreateResponse
                    m = Mmap.mmap(of, Vector{UInt8})
                    hash = SHA.sha256(m)
                    file = SimpleFileServer.File(hash, hash, length(m))
                    for client =map( x ->  SimpleFileServer.Client.make(x...),  r.replicas)
                        SimpleFileServer.upload(client, file, m)
                    end
                    r = Cairn.command_result(master, Cairn.RPC.AppendChunksRequest(object, [hash]))::Cairn.RPC.AppendChunksRequest
                end
            end
        end
        # r = Cairn.command_result(master, Cairn.RPC.AppendChunksRequest(object, [hash]))::Cairn.RPC.AppendChunksRequest ? What to append to this one..
    end
    # Notify done?
end
    
function Handle(job::ReduceJob, conn::TCPSocket)
    mktempdir() do d
        master = Cairn.Client.find_connection(job.dfs)
        exe_f, _ = mktemp(d)
        close(_)
        Cairn.Client.download(master, [job.executable], d, [exe_f])
        UDF = evalfile(map_f) #TODO Note, horrbily insecure! Also TODO: Implement non-jl files


        f_ch = Channel{ASCIIString}(3)
        location = mktempdir(d)
        @async Cairn.Client.download(master, job.files, location, Nullable(f_ch))

        mktempdir() do  results
            count = length(job.files)
            # Run UDF in parallel (hopefull better disk utlization?)
            @sync while count != 0
                count -= 1
                orig, f = take!(f_ch)
                @async begin
                    UDF(orig, f, results)
                    # upload immediately
                    object = "$(job.results_dir).$orig"
                    of = joinpath(results, name)
                    r = Cairn.command_result(master, Cairn.RPC.CreateRequest(object, job.replication))::Cairn.RPC.CreateResponse
                    m = Mmap.mmap(of, Vector{UInt8})
                    hash = SHA.sha256(m)
                    file = SimpleFileServer.File(hash, hash, length(m))
                    for client =map( x ->  SimpleFileServer.Client.make(x...),  r.replicas)
                        SimpleFileServer.upload(client, file, m)
                    end
                    r = Cairn.command_result(master, Cairn.RPC.AppendChunksRequest(object, [hash]))::Cairn.RPC.AppendChunksRequest
                end
            end

            # Do Shuffle
            @sync for name = readdir(results)
            end
        end
    end
    # No need to notify done! will be implicit
end


function RPCServer(node::t)
    server = listen(node.port)
    @info "RPC Server Starting on port $(node.port))"
    while true
        conn = accept(server)
        @async begin
            while true
                try
                    args = deserialize(conn)::BaseMessage
                    @info "RPCServer $(node)) Args: $args"
                    try
                        r = Handle(args, conn)
                        @info "RPCServer $(node) Response: $r"
                        serialize(conn,r)
                        flush(conn)
                    catch err
                        @debug "RPCServer $(node) Error Handling:\n    $args\n     $err"
                        close(conn)
                        break
                    end
                catch err
                    @debug "RPCServer $(node) Connection Failed With $err"
                    close(conn)
                    break
                    # finally
                    #     close(conn)
                end
            end
        end
    end
end

function run_job()
    # TODO: Download Code, Download files from DFS and begin streaming them into the Map phase
    # TODO: Create a module for maintaining a directory with keys
    # TODO: Use the DFS to perform a shuffle
    # TODO: Get a portion of the keys to reduce
    # TODO: Run reduce on said key
end

    
function start(p::Int64)
    RPCServer(t(p))
end


end
