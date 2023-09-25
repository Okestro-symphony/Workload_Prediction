import kfp
from kfp import dsl
from kfp import components
from kfp.components import func_to_container_op
from elasticsearch import Elasticsearch
import kubernetes.client
from kubernetes.client.models.v1_toleration import V1Toleration
from kubernetes.client.models.v1_node_selector import V1NodeSelector
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm
from kubernetes.client.models.v1_affinity import V1Affinity
from kubernetes.client.models.v1_node_affinity import V1NodeAffinity
from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement

client = kfp.Client(host='IP address')




def decomposition(provider: str,
                metric: str,
                host_thr: int=60):
    import sys
    sys.path.append('/symphony/croffle/pipelines/prediction/')

    from decomposition_main import main

    main(provider=provider, metric=metric, host_thr=host_thr)
    
 
decomposition_component = components.create_component_from_func(
        func=decomposition,
        base_image='okestroaiops/prediction:latest'
    )



@dsl.pipeline(
    name="croffle-decomposition",
    description = "croffle decompositio pipeline"
)
def create_decomposition_task(name_suffix, host_thr, cpu_request, cpu_limit, memory_request, memory_limit, vop,
                              mount_path):
    task = decomposition_component('vm', name_suffix, host_thr) \
        .set_cpu_limit(cpu_limit) \
        .set_memory_limit(memory_limit) \
        .set_cpu_request(cpu_request) \
        .set_memory_request(memory_request) \
        .add_pvolumes({mount_path: vop})
    return task


def decomposition_pipeline(
        cpu_request="4000m",
        cpu_limit="8000m",
        memory_request="4000Mi",
        memory_limit="16000Mi",
        host_thr=60
):
    dsl.get_pipeline_conf().set_image_pull_secrets([V1LocalObjectReference(name="okestroaiops")])
    vop = dsl.PipelineVolume(pvc='croffle-pvc')
    mount_path = '/symphony/'

    decomposition_tasks = []

    task_names = [
        'diskio-write', 'diskio-read', 'network-in',
        'network-out', 'filesystem', 'cpu', 'memory'
    ]

    # Create decomposition tasks in a loop
    for name_suffix in task_names:
        task = create_decomposition_task(
            name_suffix, host_thr, cpu_request, cpu_limit, memory_request, memory_limit, vop, mount_path)

        if decomposition_tasks:
            task.after(decomposition_tasks[-1])

        decomposition_tasks.append(task)

    dsl.get_pipeline_conf().set_ttl_seconds_after_finished(20)


    

kfp.compiler.Compiler().compile(
    pipeline_func=decomposition_pipeline,
    package_path='decomposition_pl.yaml'
)

client.create_recurring_run(
    experiment_id = client.get_experiment(experiment_name="default").id,
    job_name="decomposition",
    description="version: croffle:decomposition_pvc",
    cron_expression="0 0 13 * * *",
    pipeline_package_path = "decomposition_pl.yaml",
)


client.upload_pipeline(
    pipeline_package_path='decomposition_pl.yaml',
    pipeline_name = "croffle-decomposition_pvc",
    description = "version: croffle:decomposition"
)
