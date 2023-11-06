import pickle
import cloudpickle
from coffea import processor
from dask.distributed import Client
from my_module.processors.tt_processor import TTProcessor

import my_module

if __name__ == "__main__":
    # fileset
    fname = "root://xcache//store/mc/RunIISummer20UL17NanoAODv2/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_mc2017_realistic_v8-v1/120000/2A2F4EC9-F9BB-DF43-B08D-525B5389937E.root"
    fileset = {"tt": [fname]}

    # register module
    cloudpickle.register_pickle_by_value(my_module)
    
    # run processor
    out = processor.run_uproot_job(
        fileset,
        treename="Events",
        processor_instance=TTProcessor(),
        executor=processor.dask_executor,
        executor_args={
            "schema": processor.NanoAODSchema,
            "client": Client("tls://localhost:8786")
        },
    )
    # save output
    with open("outfiles/tt.pkl", "wb") as handle:
        pickle.dump(out, handle, protocol=pickle.HIGHEST_PROTOCOL)