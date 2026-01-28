import logging

from typing import List
import torch
logger = logging.getLogger()
class ModelEnsemble(torch.nn.Module):
    def __init__(self, model, n_models):
        '''
        Class to parallelize torchscript models using fork()
        If there are more than 1 CPU, the input tensor will be split in n_models tensors.
        Each tensor part will be the parameter of a model. Models will be executed in parallel using fork().
        If there is only 1 CPU, the model will be executed in serial mode.

        :param model:
        :param n_models:
        '''
        super().__init__()
        self.n_models = n_models
        self.models = torch.nn.ModuleList([model for _ in range(self.n_models)])
        self.num_real_cpus = torch.multiprocessing.cpu_count()

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # Launch tasks for each model and segment tensor
        if self.num_real_cpus==1:
            results = self.models[0].forward(x)
        else:
            # Divide in segments
            segment_size = x.shape[0] // self.n_models

            #Calculate remainder and distribute it
            remainder = x.shape[0] % self.n_models
            if remainder>0:
                segment_size=segment_size+1

            # Split the tensor into segment tensors
            segments = torch.split(x, segment_size, dim=0)


            futures: List[torch.jit.Future[torch.Tensor]] = []
            for i, model in enumerate(self.models):
                if (self.n_models == len(segments) or (self.n_models > len(segments) and i < len(segments))):
                    segment = segments[i]
                    future = torch.jit.fork(model, segment)
                    futures.append(future)

            # Collect futures
            results: List[torch.Tensor] = []
            for future in futures:
                result = torch.jit.wait(future)
                results.append(result)

            # Cat the results
            results = torch.cat(results, dim=0)

        return results