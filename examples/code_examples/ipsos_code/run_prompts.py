# Core Python Imports
import os
import logging
import json
import csv
from time import time
from itertools import product
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict

# Environment Variable Import
from dotenv import load_dotenv

# OpenAI and Response Format Imports
from openai import AzureOpenAI
from pydantic import BaseModel, ValidationError

# Local File Imports
from prompt_building import get_one_prompt
from prompt_constants import USERTYPES, MINDSETS, EXPERTISES


# ==================================================================
# Basic pre-runtime setup
# ==================================================================

# Load the environment variables from .env
load_dotenv(Path(__file__).parent.resolve() / ".env")

# Set configuartion for the progress logger in the terminal
logging.basicConfig(
    level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format = "%(message)s",
)
# ==================================================================
# ==================================================================


# ==================================================================
# These schemas are used to ensure that the llm responds with 
# the exact format that we want it to
# ==================================================================
class WithObjectResponseSchema(BaseModel):
    idea_name: str
    background: str
    expertise: str
    mindset: str
    selected_object: str
    detailed_idea_overview: str
    comprehensive_solution_description: str

class NoObjectResponseSchema(BaseModel):
    idea_name: str
    background: str
    expertise: str
    mindset: str
    detailed_idea_overview: str
    comprehensive_solution_description: str
# ==================================================================
# ==================================================================



def get_response_schema(mindset: str) -> type[BaseModel]:
    """Accepts a mindset and returns the enforced response schema given the mindset
    
    Args:
        mindset (str): Mindset option from MINDSETS
    
    Returns:
        BaseModel: Schema to enforce for LLM response
    """

    if mindset == "object_oriented":
        return WithObjectResponseSchema
    else:
        return NoObjectResponseSchema


def construct_all_prompts() -> List[Tuple[Tuple[str, str, str], str]]:
    """Builds all possible prompts we will send to Azure OpenAI

    Returns:
        Returns a list of tuples. Each tuple is of the form ((usertype, mindset, expertise), prompt)
    """

    # Iterate over all of the combinations of usertype, mindset and expertise
    prompt_tuples = []
    for combination in product(USERTYPES, MINDSETS, EXPERTISES):
        
        # Construct the full prompt for each triplet
        prompt = get_one_prompt(*combination)
        
        # Append the tuple to the return list
        prompt_tuples.append((combination, prompt))
    
    # Return the list containing all possible prompts
    return prompt_tuples


def validate_response(llm_response: str, combo: Tuple[str, str, str]) -> bool:
    """Validates that the model followed the required schema
    
    Args:
        llm_response (str): The response JSON from the LLM
        combo (Tuple[str, str, str])
    
        
    Returns:
        bool: True if the proper format is followed and is valid JSON
    """

    try:
        parsed_response = json.loads(llm_response)

        # The combo tuple is ordered as (usertype, mindset, expertise).
        # We choose the schema off the mindset so object_oriented expects selected_object.
        _, mindset, _ = combo
        response_schema = get_response_schema(mindset)
        response_schema.model_validate(parsed_response)
        return True
    except (json.JSONDecodeError, ValidationError, TypeError):
        return False


def run_one_prompt(client: AzureOpenAI, deployment: str, prompt_tuple: Tuple[Tuple[str, str, str], str]) -> Dict | None:
    """Runs a single prompt through Azure OpenAI
    
    Args:
        client (AzureOpenAI): Azure OpenAI client through which we send requests
        deployment (str): The specific deployment we are hitting in our Azure OpenAI resource
        prompt_tuple (Tuple[Tuple[str, str, str], str]): Tuple containing the triplet and the associated prompt

    Returns:
        Dict | None: The response object containing the model response and data about the initial triplet and usage info for billing. If response fails then return None.
    """

    # Unpack the tuple into the (usertype, mindset, expertise) combo and the associated prompt
    (usertype, mindset, expertise), prompt = prompt_tuple
    combo = (usertype, mindset, expertise)

    # Build the output filename
    combo_name = f"{usertype}_{mindset}_{expertise.replace(" ", "-")}"

    for attempt in range(1, int(os.getenv("MAX_ATTEMPTS", "1")) + 1):
        
        # Send the prompt to Azure OpenAI
        response = client.chat.completions.parse(
            model           = deployment,
            messages        = [{"role": "user", "content": prompt}],
            response_format = get_response_schema(mindset)
        )
    
        # Extract the text from the response as JSON string
        llm_response_text = response.choices[0].message.content
        try:
            llm_response = json.loads(llm_response_text)
        except json.JSONDecodeError:
            is_valid_response = False
            continue

        # Check if the LLM's response is valid and follows our schema
        is_valid_response = validate_response(llm_response_text, combo)

        # If response is valid then exit the loop
        if is_valid_response:
            break
    
    # Return the combo if we never got a valid response so we can write the error
    if not is_valid_response:
        return combo, attempt

    # If success then return a formatted dictionary with the response + metadata
    return {
        "backend_usertype": usertype,
        "backend_mindset": mindset,
        "backend_expertise": expertise,
        **llm_response,
        "prompt": prompt,
        "input_tokens": response.usage.prompt_tokens,
        "output_tokens": response.usage.completion_tokens,
    }, attempt, combo_name


def main():

    # Get start time for logging total runtime
    start_time = time()

    # Grab deployment name, number of requests at a time and number of total prompts to run from env vars
    deployment = os.environ["AZURE_DEPLOYMENT"]
    n_in_flight = int(os.environ.get("N_IN_FLIGHT", "4"))
    m_prompts = int(os.environ.get("PROMPT_LIMIT", "4")) 

    # Define where we will save the results
    output_results_dir = Path(__file__).parent.resolve() / "run_results"
    output_results_dir.mkdir(exist_ok = True)

    # Build the client for sending requests to Azure OpenAI
    client = AzureOpenAI(
        api_version    = os.environ["API_VERSION"],
        azure_endpoint = os.environ["AZURE_ENDPOINT"],
        api_key        = os.environ["API_KEY"],
    )

    # Construct all combinations of user type, mindset, expertise and build all associated prompts
    prompt_tuples = construct_all_prompts()
    logging.info(f"Attempting to run {m_prompts} of {len(prompt_tuples)} total prompts")

    # Restrict to max prompts to send if that is specified 
    # "m_prompts = 0" ==> "Run all prompts"
    # "m_prompts = 5" ==> "Run the first 5 prompts"
    if m_prompts > 0:
        prompt_tuples = prompt_tuples[:m_prompts]

    # Create a list of Dicts where each dict is a single response
    # We will populate this list with the responses from Azure OpenAI + metadata
    results: List[Dict] = []

    # Set up success and failure tracking variables
    successes = 0
    failures = 0
    total_attempts = 0

    # Set up total input and output tokens for logging and cost analysis
    total_input_tokens = 0
    input_cost_per_million = 1.75
    total_output_tokens = 0
    output_cost_per_million = 14.00


    # This is where we setup the ability to send multiple requests to Azure OpenAI at a time
    # Total number of requests is determined by n_in_flight
    # n_in_flight = 3 ===> 3 concurrent requests allowed at a time 
    with ThreadPoolExecutor(max_workers = n_in_flight) as executor:
        
        # Pre-define the list of jobs to run and put them in a list
        # In our case, each "job" is one request to Azure OpenAI
        # Each element of the list contains...
        #   1. The FUNCTION to call (run_one_prompt)
        #   2. The client, deployment, and prompt_tuple are the ARGUMENTS to the run_one_prompt FUNCTION 
        futures = []
        for prompt_tuple in prompt_tuples:
            futures.append(executor.submit(run_one_prompt, client, deployment, prompt_tuple))
        

        # Let each "future" (aka job) run and grab the results/log them
        for i, future in enumerate(as_completed(futures)):
            
            # Unpack results from the run_one_prompt() function
            result, num_attempts, combo_name = future.result()

            # Track total attempts across all prompts
            total_attempts += num_attempts
            
            # Check if response is a success (dict) or failure (tuple)
            # A successful result is a dict containing response data
            # A failed result is a tuple of (usertype, mindset, expertise)
            is_success = isinstance(result, dict)
            
            # Always track tokens when available, even for failed requests
            if is_success:
                results.append(result)
                successes += 1
                total_input_tokens += int(result["input_tokens"])
                total_output_tokens += int(result["output_tokens"])

                # Save the result of this successful run as its own file in the output folder
                # ---------------------------------------------------------------------------
                # Start by defining the save location (INCLUDING FILENAME!!)
                output_result_path = output_results_dir / f"{(i+1):03d}_{combo_name}.json"

                # Open the location as a new file and save the output data there
                with open(output_result_path, "w", encoding = "utf-8") as f:
                    json.dump(result, f, indent = 4, ensure_ascii = False)
                # ---------------------------------------------------------------------------
                
                # Log every 50th successful response
                if successes % 50 == 0:
                    logging.info("Prompt %s completed | Combo: %s | In: %s | Out: %s", successes, result.get("backend_usertype"), result["input_tokens"], result["output_tokens"])
            else:
                # Result is a tuple of (usertype, mindset, expertise) from failed request
                failures += 1
                logging.warning("Prompt %s failed after %s attempts: %s | %s | %s", i, num_attempts, result[0], result[1], result[2])
                
                # Write error data to a separate CSV for review
                error_file_path = Path(__file__).parent.resolve() / "failures.csv"
                with open(error_file_path, mode = "a", encoding = "utf8") as f:
                    writer = csv.writer(f)
                    
                    # Check file size to determine if we need a header
                    # If you are curious.... here is an explainer of a computer/coding concept that took me a long while to get, but
                    # once I understood it I came to understand computers in a much better way. In particular I am talking about the
                    # f.tell() function used below. In all honesty I came to understand this via some C# code I had to review/understand
                    # at my job in Croatia, but this moment provides an opportunity to explain that concept....
                    # 
                    # The important insight: Computers are simple and stupid. However, they are deeply careful implementations of simple, stupid ideas.
                    #           
                    # So you look at this snippet of code `if f.tell() == 0` and you might have no clue what it means. 
                    # Perhaps you start by trying to find out what each variable is (i.e. `f`) and what method it calls (In this case it calls the `tell()` method).
                    #       (Note: I definitely reccomend googling or chatGPT-ing what a method is if you do not know. Super high payoff relative to conceptual load)
                    #               
                    #               My Recommended Prompt 
                    #               ---------------------- 
                    #               What is a "method" in programming? I have a line of code that says "if f.tell() == 0" and I do not understand what is happening. 
                    #               It looks like it is calling a function or doing something, but I do not get how a variable can like call a function too?
                    #               If it is a function then why isn't it tell(f)? What is f? Why is there a period (".")? What does that do? What is going on here?   
                    #               Here is the context in which I found this code....
                    #               {COPY PASTE ALL CODE CONTEXT HERE}
                    # 
                    # 
                    #       P.S. I use this basic prompt structure for many of my prompts when I just want to understand a new thing...
                    #   
                    #  But why would something equaling 0 represent a file being "new"? 
                    #       Is it that the file has 0 data in there? Sort of, but not exactly.
                    # 
                    # The answer: If your code has loaded a whole file, then it must have "read" all of it! 
                    #     The fact that all of the file is available to the code means that it must have read all of it.
                    #     Although computers *seem* magical, they are not *so* magical that they know what is in a file without reading all of it!
                    #     But how do computers read files? They read them one letter at a time, just like you!
                    #     It starts with a "pointer" that points to the start of a file and reads the whole thing step by step. 
                    #     But, MUCH LIKE A VCR, it does not automatically reset itself once it is done.
                    #     So if your pointer read the whole file and is still at position 0, then the file must be empty!
                    #     That is where this code's logic comes from.
                    #     The translation of `if f.tell() == 0, ` into english is "If you read the whole file and you are exactly where you started, then the file is empty and we need to write a header!"
                    if f.tell() == 0:
                        writer.writerow(["Prompt Number", "User Type", "Mindset", "Expertise", "Attempts"])
                    
                    writer.writerow([str(i), result[0], result[1], result[2], num_attempts])
                    
                

    # Calculate general stats
    total_prompts_sent = len(results) + failures
    total_runtime = time() - start_time
    
    # Calculate costs
    total_input_token_cost = (total_input_tokens / 10**6) * input_cost_per_million
    total_output_token_cost = (total_output_tokens / 10**6) * output_cost_per_million
    total_cost = total_input_token_cost + total_output_token_cost
    cost_per_prompt = total_cost / successes if successes > 0 else 0
    
    # Prepare summary data for JSON export
    summary_data = {
        "run_metadata": {
            "total_prompts_sent": total_prompts_sent,
            "successes": successes,
            "failures": failures,
            "total_attempts": total_attempts,
            "total_runtime_seconds": total_runtime,
            "runtime_per_prompt_seconds": total_runtime / total_prompts_sent if total_prompts_sent > 0 else 0,
        },
        "token_usage": {
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_input_tokens + total_output_tokens,
        },
        "cost_analysis": {
            "input_cost_per_million": input_cost_per_million,
            "output_cost_per_million": output_cost_per_million,
            "total_input_cost": total_input_token_cost,
            "total_output_cost": total_output_token_cost,
            "total_cost": total_cost,
            "cost_per_prompt": cost_per_prompt,
        },
    }

    
    # Write summary JSON to root
    output_json_path = Path(__file__).parent.resolve() / "run_summary.json"
    with open(output_json_path, "w", encoding = "utf-8") as f:
        json.dump(summary_data, f, indent = 4, ensure_ascii = False)

    # Summary info header
    logging.info("=" * 60)
    logging.info("SUMMARY")
    logging.info("=" * 60)

    # General statistics such as runtime
    logging.info("-" * 60)
    logging.info("General Statistics")
    logging.info("-" * 60)
    
    logging.info("Total Prompts Sent: %s", total_prompts_sent)
    logging.info("Successes: %s", successes)
    logging.info("Failures: %s", failures)
    logging.info("Total Attempts: %s", total_attempts)
    logging.info("Total Runtime: %s sec", total_runtime)
    logging.info("Runtime per Prompt: %s sec", total_runtime / total_prompts_sent if total_prompts_sent > 0 else 0)
    logging.info("-" * 60)

    # Cost analysis statistics
    logging.info("Cost Analysis")
    logging.info("-" * 60)

    logging.info("Total Input Tokens: %s", total_input_tokens)
    logging.info("Total Output Tokens: %s", total_output_tokens)
    logging.info("Total Tokens: %s", total_input_tokens + total_output_tokens)
    logging.info("Input Token Cost: $%s", total_input_token_cost)
    logging.info("Output Token Cost: $%s", total_output_token_cost)
    logging.info("Total Cost: $%s", total_cost)
    logging.info("Cost per Prompt: $%s", cost_per_prompt)
    logging.info("-" * 60)
    logging.info("Per-prompt outputs saved to: %s", output_results_dir)
    logging.info("Summary saved to: %s", output_json_path)
    logging.info("=" * 60)


if __name__ == "__main__":
    main()