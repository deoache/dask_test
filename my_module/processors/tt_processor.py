import copy
import hist
import json
import importlib.resources
import awkward as ak
from coffea import processor


class TTProcessor(processor.ProcessorABC):
    def __init__(self):
        # define accumulator object 
        jet_pt_axis = hist.axis.Regular(
            bins=50, start=20, stop=1000, name="jet_pt", label="Jet $p_T$ [GeV]"
        )
        self.output = hist.Hist(jet_pt_axis)
        
    
    def process(self, events):
        # create copies of histogram objects
        output = copy.deepcopy(self.output)
        
        # open and load btagDeepFlavB working point
        #with importlib.resources.open_text("my_module.data", "btagWPs.json") as file:
        #    btag_threshold = json.load(file)["deepJet"]["2017"]["M"]
        btag_threshold = 0.34
        
        # select good jets
        jets = events.Jet[(events.Jet.pt > 20) & (events.Jet.btagDeepFlavB > btag_threshold)]

        # get leading jet pt
        leading_jet_pt = ak.firsts(jets).pt
        
        # fill output histogram
        output.fill(jet_pt=ak.fill_none(leading_jet_pt, 0))
        
        return output

    def postprocess(self, accumulator):
        return accumulator