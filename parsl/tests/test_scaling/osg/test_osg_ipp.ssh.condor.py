from parsl import *
import parsl
import libsubmit
#libsubmit.set_stream_logger()
#parsl.set_stream_logger()

config = {
    "sites" : [
        { "site" : "OSG_Remote",
          "auth" : {
              "channel" : "ssh",
              "hostname" : "login.osgconnect.net",
              "username" : "yadunand",
              "scriptDir" : "/home/yadunand/parsl_scripts/"
              #"scriptDir" : "/tmp/parsl_scripts/"
          },
          "execution" : {
              "executor" : "ipp",
              "provider" : "condor",  # LIKELY SHOULD BE BOUND TO SITE
              "scriptDir" : ".scripts",
              "block" : { # Definition of a block
                  "nodes" : 1,            # of nodes in that block
                  "taskBlocks" : 1,       # total tasks in a block
                  "walltime" : "00:05:00",
                  "initBlocks" : 1,
                  "minBlocks" : 0,
                  "maxBlocks" : 1,
                  "scriptDir" : ".",
                  "options" : {
                      "partition" : "debug",
                      "overrides" : "",
                      "workerSetup" : """module load python/3.5.2;
python3 -m venv parsl_env;
source parsl_env/bin/activate;
pip3 install ipyparallel """
                  }
              }
          }
        }
    ],
    "globals" : {"lazyErrors" : True },
    "controller" : { "publicIp" : "128.135.250.229" }
}

dfk = DataFlowKernel(config=config)

@App("python", dfk)
def test():
    import platform
    return "Hello from {0}".format(platform.uname())


if __name__ == "__main__" :

    results = {}
    for i in range(0,5):
        results[i] = test()

    print("Waiting ....")
    print(results[0].result())
