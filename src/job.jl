module Job
using Cairn.Client

using Cairn.RPC: PrefixKey, Key, KeySpan

immutable t
    # NOT TRUE; FUTURE WORK. ALL MUST BE .jl
    # if *_executable ends in .jl, then it will try to be
    # imported. Otherwise, it will be piped via stdin
    dfs::Vector{Cairn.Client.t}
    #=
    A mapfn executable should have a single function declaration
    that takes an original file name, an actual file name, a results dir
    it should read from the actual file name
    and write output files of the form <results dir>/hash(ouput key) or <results dir>/key
    depending on the key type
    =#
    map_executable::Key
    reduce_executable::Key
    files::KeySpan
    workers::Int64
    result_replication::Int64
    result_dir::PrefixKey
    job_id::ASCIIString
    function t(dfs::Cairn.Client.t, map_exe::ASCIIString,
               reduce_exe::ASCIIString, data::FileSpan,
               local_data::Vector{ASCIIString}, workers::Int64,
               exe_replication::Int64, upload_replication::Int64,
               result_replication::Int64,
               results_dir::PrefixKey = PrefixKey(string(uuid4())),
               job_id::ASCIIString = string(uuid4()))
        # upload code to the DFS
        map_exe_name = string(uuid4())
        map_exe_key = "$job_id.$map_exe_name.$map_exe"
        open(map_exe, "r") do f
            mm_data = Mmap.mmap(f, Vector{UInt8})
            Cairn.Client.upload(dfs, map_exe_key,  mm_data, exe_replication)
        end
        reduce_exe_name = string(uuid4())
        reduce_exe_key = "$job_id.$reduce_exe_name.$reduce_exe"
        open(reduce_exe, "r") do f
            mm_data = Mmap.mmap(f, Vector{UInt8})
            Cairn.Client.upload(dfs, reduce_exe_key,  mm_data, exe_replication)
        end
        
        # upload files to the DFS (if not already there)
        @sync for (i,key) = enumerate(local_data)
            @async open(key, "r") do f
                data = Mmap.mmap(f, Vector{UInt8})
                Cairn.Client.upload(dfs, "$job_id.uploaded.$i",  data, upload_replication)
            end
            
        end
        if length(local_data) != 0
            data = [data; PrefixKey("$job_id.uploaded.")]
        end

        new(Cairn.Client.getMasters(dfs), map_exe_key, reduce_exe_key, data, workers, results_replication, results_dir, job_id)
    end
    t(job::t, sub::FileSpan) = new(job.dfs, job.map_executable, job.reduce_executable, sub, 0,
    job.result_replication, job.result_dir, job.job_id)
end


end
