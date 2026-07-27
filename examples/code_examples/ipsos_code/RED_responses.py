# Core Python Imports
import os
import logging
import json
import csv
from time import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple, Any

# Environment Variable Import
from dotenv import load_dotenv

# OpenAI and Response Format Imports
from openai import AzureOpenAI
from pydantic import BaseModel, ValidationError


# ==================================================================
# Basic pre-runtime setup
# ==================================================================

# Load the environment variables from .env
load_dotenv(Path(__file__).parent.resolve() / ".env")

# Set configuration for the progress logger in the terminal
logging.basicConfig(
    level  = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format = "%(message)s",
)
# ==================================================================
# ==================================================================


# ==================================================================
# This schema is used to ensure that the llm responds with
# the exact format that we want it to
# ==================================================================
class ReformattedConceptSchema(BaseModel):
    concept_name: str
    insight_statement: str
    benefit_statement: str
    reason_to_believe: str
# ==================================================================
# ==================================================================


# ==================================================================
# Prompt construction and response validation helpers
# ==================================================================
def build_previous_output_block(source_result: Dict[str, Any]) -> str:
    """Builds a clean, readable block from one prior idea output.

    Args:
        source_result (Dict[str, Any]): Parsed JSON payload from one file in run_results

    Returns:
        str: Readable text block representing the previous idea output
    """

    # Build ordered lines so the model sees a stable, predictable structure.
    lines = [
        f"Idea Name: {source_result.get('idea_name', '')}",
        f"Background: {source_result.get('background', '')}",
        f"Expertise: {source_result.get('expertise', '')}",
        f"Mindset: {source_result.get('mindset', '')}",
    ]

    # Include selected_object only when present in source payload.
    if "selected_object" in source_result:
        lines.append(f"Selected Object: {source_result.get('selected_object', '')}")

    lines.extend([
        f"Idea Summary: {source_result.get('detailed_idea_overview', source_result.get('idea_summary', ''))}",
        f"Idea Explanation: {source_result.get('comprehensive_solution_description', source_result.get('idea_explanation', ''))}",
    ])

    return "\n\n".join(lines)


def build_reformat_prompt(previous_output_block: str) -> str:
    """Builds the exact reformatting prompt with inserted previous output.

    Args:
        previous_output_block (str): Main prompt output from the prior run

    Returns:
        str: Full prompt text sent to the LLM
    """

    return f"""
We would like you to summarize and format the previous idea as follows. Do not include any details about the innovator who came up with the solution. Make sure it is clear what the described product would look and feel like. Do not add any of your own ideas: try to draw entirely on what is included above.

Your response should be around 200 words and easy to read. Each section should be less than 80 words. Do not explicitly mention word count in your response.
OUTPUT FORMAT
Concept Name
Provide the name of the concept, followed by a short tagline on the same line.


Insight Statement
Write a short consumer-centered insight that captures the core tension behind the idea.
This should be written in a natural, human voice while remaining specific and grounded in the lived reality described in the tensions and transcript.
Structure it in the following form:
I... because... but...
This statement should:
express what the consumer is trying to do or protect
explain why that matters in their daily life
reveal the conflict, frustration, or unmet need that creates the opportunity for innovation
The insight should feel psychologically true and closely tied to medication storage behavior in Malaysia.
Benefit Statement
Explain what the concept does for consumers and why it matters.
This should clearly describe both:
the rational benefit (how the packaging improves storage, safety, clarity, usability, or reliability)
the emotional benefit (how it helps the consumer feel more reassured, capable, in control, prepared, or less burdened)
Write this as a concise but polished paragraph.
Focus on the value created for the user, not on technical specifications alone.
Reason to Believe
Explain why the consumer should believe in the concept.
This section should make the idea feel credible, concrete, and buildable.
It should include:
the key packaging features or mechanisms that make the concept work
why these features solve the identified tension
why the concept is feasible for an over-the-counter medicine company to produce or adopt
The explanation should be specific enough to show how the concept could realistically be developed, while still being written in a persuasive and concept-level style rather than as a technical spec sheet.
A member of the general public should be able to read and easily digest what you write. Do not use any intimidating or difficult technical language.

Here is the previous idea output to summarize and reformat:
#####
{previous_output_block}
%%%%%%

Return only valid JSON in this exact shape:
{{
  "concept_name": "<Concept Name + short tagline on the same line>",
  "insight_statement": "<Insight statement in the required I... because... but... form>",
  "benefit_statement": "<Benefit statement paragraph>",
  "reason_to_believe": "<Reason to believe paragraph>"
}}
"""


def validate_response(llm_response: str) -> bool:
    """Validates that the model followed the required schema.

    Args:
        llm_response (str): The response JSON from the LLM

    Returns:
        bool: True if proper format is followed and response is valid JSON
    """

    try:
        parsed_response = json.loads(llm_response)
        ReformattedConceptSchema.model_validate(parsed_response)
        return True
    except (json.JSONDecodeError, ValidationError, TypeError):
        return False
# ==================================================================
# ==================================================================


# ==================================================================
# Runtime helpers for loading source files and running prompts
# ==================================================================
def get_source_files(source_dir: Path, m_prompts: int) -> List[Path]:
    """Returns sorted source files from the source result directory.

    Args:
        source_dir (Path): Directory containing prior per-prompt JSON files
        m_prompts (int): Optional limit on number of files to process

    Returns:
        List[Path]: Sorted list of JSON files to process
    """

    source_files = sorted(source_dir.glob("*.json"))

    # Restrict to max files to process if specified.
    # "m_prompts = 0" ==> "Process all files"
    # "m_prompts = 5" ==> "Process first 5 files"
    if m_prompts > 0:
        source_files = source_files[:m_prompts]

    return source_files


def run_one_prompt(client: AzureOpenAI, deployment: str, source_file: Path) -> Tuple[Dict[str, Any] | Tuple[str, str], int]:
    """Runs one reformatting prompt through Azure OpenAI.

    Args:
        client (AzureOpenAI): Azure OpenAI client used for requests
        deployment (str): Deployment name in the Azure OpenAI resource
        source_file (Path): Source JSON file containing one prior main idea output

    Returns:
        Tuple[Dict[str, Any] | Tuple[str, str], int]:
            Success => (result_dict, num_attempts)
            Failure => ((source_file_name, error_label), num_attempts)
    """

    with open(source_file, "r", encoding = "utf-8") as f:
        source_result = json.load(f)

    previous_output_block = build_previous_output_block(source_result)
    prompt = build_reformat_prompt(previous_output_block)

    is_valid_response = False
    llm_response = {}

    for attempt in range(1, int(os.getenv("MAX_ATTEMPTS", "1")) + 1):

        # Send the prompt to Azure OpenAI.
        response = client.chat.completions.parse(
            model           = deployment,
            messages        = [{"role": "user", "content": prompt}],
            response_format = ReformattedConceptSchema,
        )

        # Extract the text response and validate it.
        llm_response_text = response.choices[0].message.content
        try:
            llm_response = json.loads(llm_response_text)
        except json.JSONDecodeError:
            is_valid_response = False
            continue

        is_valid_response = validate_response(llm_response_text)

        # If response is valid then exit the loop.
        if is_valid_response:
            break

    # Return failure payload if valid format was never produced.
    if not is_valid_response:
        return (source_file.name, "invalid_or_unparseable_response"), attempt

    # Return a formatted success dictionary with response + metadata.
    return {
        "source_file": source_file.name,
        "source_idea_name": source_result.get("idea_name", ""),
        "source_backend_usertype": source_result.get("backend_usertype", ""),
        "source_backend_mindset": source_result.get("backend_mindset", ""),
        "source_backend_expertise": source_result.get("backend_expertise", ""),
        **llm_response,
        "prompt": prompt,
        "input_tokens": response.usage.prompt_tokens,
        "output_tokens": response.usage.completion_tokens,
    }, attempt
# ==================================================================
# ==================================================================



def main():

    # Get start time for logging total runtime.
    start_time = time()

    # Grab deployment name, number of requests at a time, and prompt limit from env vars.
    deployment = os.environ["AZURE_DEPLOYMENT"]
    n_in_flight = int(os.environ.get("N_IN_FLIGHT", "4"))
    m_prompts = int(os.environ.get("PROMPT_LIMIT", "0"))

    # Define source and output paths.
    source_results_dir = Path(os.environ.get("SOURCE_RESULTS_DIR", "run_results")).resolve()
    output_results_dir = Path(os.environ.get("OUTPUT_RESULTS_DIR", "RED_results")).resolve()
    output_summary_path = Path(os.environ.get("OUTPUT_SUMMARY_PATH", "run_summary_reformatted.json")).resolve()
    failures_csv_path = Path(os.environ.get("FAILURES_CSV_PATH", "failures_reformatted.csv")).resolve()

    # Build the client for sending requests to Azure OpenAI.
    client = AzureOpenAI(
        api_version    = os.environ["API_VERSION"],
        azure_endpoint = os.environ["AZURE_ENDPOINT"],
        api_key        = os.environ["API_KEY"],
    )

    # Build source file list.
    source_files = get_source_files(source_results_dir, m_prompts)

    if len(source_files) == 0:
        logging.warning("No source files found in: %s", source_results_dir)
        return

    # Create output directory if needed.
    output_results_dir.mkdir(exist_ok = True)

    # Create a list of Dicts where each dict is a single successful response.
    results: List[Dict[str, Any]] = []

    # Set up success and failure tracking variables.
    successes = 0
    failures = 0
    total_attempts = 0

    # Set up total input and output tokens for logging and cost analysis.
    total_input_tokens = 0
    input_cost_per_million = 1.75
    total_output_tokens = 0
    output_cost_per_million = 14.00

    # This is where we setup the ability to send multiple requests at a time.
    # Total number of concurrent requests is determined by n_in_flight.
    with ThreadPoolExecutor(max_workers = n_in_flight) as executor:

        # Pre-define and submit all jobs.
        futures = []
        for source_file in source_files:
            futures.append(executor.submit(run_one_prompt, client, deployment, source_file))

        # Let each future run and grab the results/log them.
        for i, future in enumerate(as_completed(futures), start = 1):

            # Unpack results from the run_one_prompt() function.
            result, num_attempts = future.result()

            # Track total attempts across all prompts.
            total_attempts += num_attempts

            # Check if response is a success (dict) or failure (tuple).
            is_success = isinstance(result, dict)

            # Always track tokens when available on successful requests.
            if is_success:
                results.append(result)
                successes += 1
                total_input_tokens += int(result["input_tokens"])
                total_output_tokens += int(result["output_tokens"])

                # Log every 50th successful response.
                if successes % 50 == 0:
                    logging.info(
                        "Prompt %s/50 completed | Source: %s | Input Tokens: %s | Output Tokens: %s",
                        successes,
                        result.get("source_file"),
                        result["input_tokens"],
                        result["output_tokens"],
                    )
            else:
                failures += 1
                logging.warning("Prompt %s failed after %s attempts: %s | %s", i, num_attempts, result[0], result[1])

                # Write error data to a separate CSV for review.
                with open(failures_csv_path, mode = "a", encoding = "utf-8") as f:
                    writer = csv.writer(f)

                    # Check file size to determine if we need a header.
                    if f.tell() == 0:
                        writer.writerow(["Prompt Number", "Source File", "Error Label", "Attempts"])

                    writer.writerow([str(i), result[0], result[1], num_attempts])

    # Calculate general stats.
    total_prompts_sent = len(results) + failures
    total_runtime = time() - start_time

    # Calculate costs.
    total_input_token_cost = (total_input_tokens / 10**6) * input_cost_per_million
    total_output_token_cost = (total_output_tokens / 10**6) * output_cost_per_million
    total_cost = total_input_token_cost + total_output_token_cost
    cost_per_prompt = total_cost / successes if successes > 0 else 0

    # Prepare summary data for JSON export.
    summary_data = {
        "run_metadata": {
            "source_results_dir": str(source_results_dir),
            "output_results_dir": str(output_results_dir),
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

    # Save one file per successful prompt result for easier downstream handling.
    for result in results:
        output_result_path = output_results_dir / result["source_file"]
        with open(output_result_path, "w", encoding = "utf-8") as f:
            json.dump(result, f, indent = 4, ensure_ascii = False)

    # Write summary JSON.
    with open(output_summary_path, "w", encoding = "utf-8") as f:
        json.dump(summary_data, f, indent = 4, ensure_ascii = False)

    # Summary info header.
    logging.info("=" * 60)
    logging.info("SUMMARY")
    logging.info("=" * 60)

    # General statistics such as runtime.
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

    # Cost analysis statistics.
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
    logging.info("Summary saved to: %s", output_summary_path)
    logging.info("Failures CSV saved to: %s", failures_csv_path)
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
