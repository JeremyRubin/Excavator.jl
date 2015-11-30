
module ComputeManager
using Cairn.Client
using Excavator: Job
function run(job::t)
    # TODO keep track of ComputeNodes
    
    # start 
        # run the job
end
immutable MapJob
    dfs::Vector{Cairn.Client.t}
    executable::Job.Key
    files::Cairn.RPC.KeySpan
    results_dir::Job.Key
    done::Job.Key # Special 0-replication file!

end

immutable ReduceJob
    dfs::Vector{Cairn.Client.t}
    executable::Job.Key
    files::Cairn.RPC.KeySpan
    results_dir::Job.Key
end


