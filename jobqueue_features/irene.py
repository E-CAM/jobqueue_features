# from dask_jobqueue.slurm import SLURMJob, SLURMCluster
# from .clusters import CustomSLURMJob, CustomSLURMCluster
from jobqueue_features.clusters import CustomSLURMJob, CustomSLURMCluster
from jobqueue_features.mpi_wrapper import CCC_MPRUN


class IreneJob(CustomSLURMJob):
    submit_command = "ccc_msub"
    cancel_command = "ccc_mdel"
    parameters_map = {
        "-p": "-q",
        "-A": "-A",
        "-m": "-m",
        "-t": "-T",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        sbatch_prefix = "#SBATCH"
        msub_prefix = "#MSUB"
        header_lines = self.job_header.split("\n")
        modified_lines = []
        slurm_parameters = []
        for line in header_lines:
            if line.startswith(sbatch_prefix):
                stripped = line.replace(f"{sbatch_prefix} ", "")
                for k, v in self.parameters_map.items():
                    if stripped.startswith(k):
                        modified_lines.append(f"{msub_prefix} {stripped.replace(k, v)}")
                        break
                else:
                    slurm_parameters.append(stripped)
            else:
                modified_lines.append(line)

        idx = modified_lines.index("")
        self.job_header = "\n".join(modified_lines[:idx])
        self.job_header += f"\n{msub_prefix} -E '{' '.join(slurm_parameters)}'\n"
        self.job_header += "\n".join(modified_lines[idx:])


class IreneCluster(CustomSLURMCluster):
    job_cls = IreneJob
