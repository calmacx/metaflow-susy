from metaflow import FlowSpec, Parameter, step

class GenerateLimits(FlowSpec):

    region = Parameter('region',
                       help="Decide which region to run.",
                       default='RegionC')


    @step
    def start(self):
        self.next(self.run_exclusion,self.run_bkg)

    @step
    def run_bkg(self):
        import json
        import copy
        import pyhf
        
        with open('{region}/BkgOnly.json'.format(region=self.region)) as fname:
            spec = json.load(fname)
            spec = copy.copy(spec)

        #switch the parameter of interest to the lumi from any signal strength
        spec["measurements"][0]["config"]["poi"] = "lumi"
        
        #load the workspace
        ws = pyhf.Workspace(spec)

        #load the model
        model = ws.model(
            measurement_name="NormalMeasurement",
            modifier_settings={
                "normsys": {"interpcode": "code4"},
                "histosys": {"interpcode": "code4p"},
            },
        )

        data = ws.data(model)
        
        self.model = model
        self.data = data
        self.next(self.fit_bkg)

    @step
    def fit_bkg(self):
        import pyhf
        result,nll = pyhf.infer.mle.fit(
            self.data, self.model, return_fitted_val=True
        )
        
        self.results = {
            'bestfit': result.tolist(),
            'nll':nll.tolist()
        }
        
        
        self.next(self.finalise)
        

    @step
    def run_exclusion(self):
        import subprocess
        import sys

        command = r'''jq -r ".patches[].metadata.name" %s/patchset.json'''%(self.region)
        self.signals = subprocess.check_output(['bash', '-c',command]).decode(sys.stdout.encoding).splitlines()
        print ('Found signals...')
        print (self.signals)
        self.next(self.get_signal_model,foreach='signals')
        
        
    @step
    def get_signal_model(self):
        import subprocess
        import pyhf
        import json
        import copy
        
        signal = self.input
        patch_command = r'''jsonpatch {region}/BkgOnly.json <(pyhf patchset extract {region}/patchset.json --name "{signal}") > {region}/{signal}.json'''
        
        subprocess.call(['bash', '-c', patch_command.format(region=self.region,signal=signal)])

        print ('Now running',signal)
        
        with open('{region}/{signal}.json'.format(region=self.region,signal=signal)) as fname:
            spec = json.load(fname)
            spec = copy.copy(spec)
            
        #load the workspace
        ws = pyhf.Workspace(spec)

        #load the model
        model = ws.model(
            measurement_name="NormalMeasurement",
            modifier_settings={
                "normsys": {"interpcode": "code4"},
                "histosys": {"interpcode": "code4p"},
            },
        )

        data = ws.data(model)
        
        self.ws = ws
        self.model = model
        self.data = data
        self.signal = signal
        self.next(self.fit_exclusion)

    @step
    def fit_exclusion(self):
        import pyhf

        print ('calculating CLs for',self.signal)
        test_mu = 1.0
        CLs_obs, CLs_exp = pyhf.infer.hypotest(test_mu, self.data, self.model, qtilde=True, return_expected=True)
        print(f"Observed: {CLs_obs}, Expected: {CLs_exp}")

        
        self.CLs_obs = CLs_obs
        self.CLs_exp = CLs_exp
        self.next(self.join_cls)

    @step
    def join_cls(self,inputs):
        self.results = [
            {
                'signal': inp.signal,
                'CLs_obs': inp.CLs_obs.tolist(),
                'CLs_exp': inp.CLs_exp.tolist()
            }
            for inp in inputs ]
        
        self.next(self.finalise)

    @step
    def finalise(self,inputs):
        self.results = {
            'signal_cls':inputs.join_cls.results,
            'bkg_fit':inputs.fit_bkg.results
            }
        self.next(self.end)
        
    @step
    def end(self):
        import json
        print (json.dumps(self.results, indent=4))
        pass

if __name__ == '__main__':
    GenerateLimits()
