import kfp
from kfp import dsl
from kfp import components
from kfp.components import func_to_container_op
from elasticsearch import Elasticsearch
import kubernetes.client
client = kfp.Client(host='ip_address')

def prediction():
    import sys
    sys.path.append('path/your/prediction')

    from prediction_main import prediction_main
    prediction_main()
    
 
prediction_component = components.create_component_from_func(
        func=prediction,
        base_image='path/your/image'
    )



@dsl.pipeline(
    name="croffle-prediction_pvc",
    description = "croffle prediction pipeline using pvc"
)
def prediction_pipeline(cpu_request :str="4000m", 
                        cpu_limit : str="8000m",
                        memory_request : str="4000Mi",
                        memory_limit : str="16000Mi"):
    dsl.get_pipeline_conf().set_image_pull_secrets([kubernetes.client.V1LocalObjectReference(name="public_aipops")])
    vop = dsl.PipelineVolume(pvc='croffle-pvc')    

    mount_path = "/aiplatform/"

    prdiction_task = prediction_component().set_cpu_limit(cpu_limit)\
                                            .set_memory_limit(memory_limit)\
                                            .set_cpu_request(cpu_request)\
                                            .set_memory_request(memory_request)\
                                            .add_pvolumes({mount_path: vop})

    dsl.get_pipeline_conf().set_ttl_seconds_after_finished(20)



client.create_run_from_pipeline_func(prediction_pipeline, arguments={})
    

kfp.compiler.Compiler().compile(
    pipeline_func=prediction_pipeline,
    package_path='prediction_pl.yaml'
)

client.create_recurring_run(
    experiment_id = client.get_experiment(experiment_name="Default").id,
    job_name="prediction_croffle pvc",
    description="version: croffle:prediction_pvc",
    cron_expression="0 0 17 * * *",
    pipeline_package_path = "prediction_pl.yaml",
)



client.upload_pipeline(
    pipeline_package_path='prediction_pl.yaml',
    pipeline_name = "prediction_croffle_pvc",
    description = "version: croffle:prediction_pvc"
)
