import time
from .base_manager import BaseManager
from .task_manager import TaskManager
from bolna.helpers.logger_config import configure_logger
from pymongo import MongoClient
import os

logger = configure_logger(__name__)
def mongodb_connection():
    """
    Establishes a connection to the MongoDB database using the environment variables 'MONGO_URL' and 'MONGO_DATABASE'.
    """
    try:
        mongo_client = MongoClient(os.getenv('MONGODB_URI'))
        db = mongo_client['test']
    except ValueError as e:
        raise ValueError(f"Error in mongodb_connection: {e.args[0]}")
    return db

db = mongodb_connection()

class AssistantManager(BaseManager):
    def __init__(self, agent_config, ws=None, assistant_id=None, context_data=None, conversation_history=None,
                 connected_through_dashboard=None, cache=None, input_queue=None, output_queue=None, **kwargs):
        super().__init__()
        self.tools = {}
        self.websocket = ws
        self.agent_config = agent_config
        self.context_data = context_data
        self.tasks = agent_config.get('tasks', [])
        self.task_states = [False] * len(self.tasks)
        self.assistant_id = assistant_id
        self.run_id = f"{self.assistant_id}#{str(int(time.time() * 1000))}"
        self.connected_through_dashboard = connected_through_dashboard
        self.cache = cache
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.kwargs = kwargs
        self.conversation_history = conversation_history

    async def run(self, local=False, run_id=None):
        """
        Run will start all tasks in sequential format
        """
        if run_id:
            self.run_id = run_id

        input_parameters = None
        for task_id, task in enumerate(self.tasks):

            logger.info(f"Running task {task_id} {task} and sending kwargs {self.kwargs}")
            task_manager = TaskManager(self.agent_config.get("agent_name", self.agent_config.get("assistant_name")),
                                       task_id, task, self.websocket,
                                       context_data=self.context_data, input_parameters=input_parameters,
                                       assistant_id=self.assistant_id, run_id=self.run_id,
                                       connected_through_dashboard=self.connected_through_dashboard,
                                       cache=self.cache, input_queue=self.input_queue, output_queue=self.output_queue,
                                       conversation_history=self.conversation_history, **self.kwargs)

            print("************************************************************************************************"*20)
            logger.info(self.agent_config.get("agent_name", self.agent_config.get("assistant_name")))
            logger.info(task_id)
            # logger.info(task)
            logger.info(f"kwargs: {self.kwargs}")
            # logger.info("context_data: ", self.context_data)
            # logger.info("input_parameters: ", input_parameters)
            logger.info(f"Running id in assistant manager #45: {self.run_id}")
            # logger.info("connected_through_dashboard: ", self.connected_through_dashboard)
            # logger.info("cache: ", self.cache)
            # logger.info("input_queue: ", self.input_queue)
            # logger.info("output_queue: ", self.output_queue)
            # logger.info("conversation_history: ", self.conversation_history)
            # logger.info("kwargs: ", self.kwargs)
            logger.info(f"transcriber duration$ #546: {task_manager.transcriber_duration}")
            # logger.info("synthesizer voice", task_manager.synthesizer_voice)
            # logger.info("synthesizer provider", task_manager.synthesizer_provider)
            # logger.info("synthesizer characters", task_manager.synthesizer_characters)
            # # logger.info("LLM providers", task_manager.task_config["tools_config"]["llm_agent"]["provider"])
            # logger.info("LLM model", task_manager.task_config["tools_config"]["llm_agent"]["model"])
            # logger.info("LLM temperature", task_manager.task_config["tools_config"]["llm_agent"]["temperature"])
            # logger.info("LLM max tokens", task_manager.task_config["tools_config"]["llm_agent"]["max_tokens"])
            # logger.info("Latency", task_manager.latency_dict)
            print("************************************************************************************************"*20)


            await task_manager.load_prompt(self.agent_config.get("agent_name", self.agent_config.get("assistant_name")),
                                           task_id, local=local, **self.kwargs)
            task_output = await task_manager.run()
            task_output['run_id'] = self.run_id
            yield task_id, task_output.copy()
            self.task_states[task_id] = True
            if task_id == 0:
                input_parameters = task_output

                # removing context_data from non-conversational tasks
                self.context_data = None
            logger.info(f"task_output {task_output}")
            logger.info(f"task_output type {type(task_output)}")
            task_output['model'] = task_manager.task_config["tools_config"]["llm_agent"]["model"]
            task_output['temperature'] = task_manager.task_config["tools_config"]["llm_agent"]["temperature"]
            task_output['max_tokens'] = task_manager.task_config["tools_config"]["llm_agent"]["max_tokens"]
            task_output['synthesizer_voice'] = task_manager.synthesizer_voice
            task_output['synthesizer_provider'] = task_manager.synthesizer_provider
            db['execution_metadata'].insert_one(task_output)
            if task["task_type"] == "extraction":
                input_parameters["extraction_details"] = task_output["extracted_data"]
        logger.info("Done with execution of the agent")
