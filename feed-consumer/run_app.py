import os
import argparse
import subprocess

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the application in a specific mode.")
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        help="Mode to start the application in. Options: 'feedEnricher', 'campaignEvaluator', 'eventActuator'."
    )
    parser.add_argument(
        "--port",
        type=str,
        required=True,
        help="Port to run the application on. Default is 8082."
    )
    args = parser.parse_args()
    valid_modes = ["feedEnricher", "campaignEvaluator", "eventActuator"]
    mode = args.mode
    port= args.port
    if not mode:
        print("Error: APP_MODE environment variable is not set. Please set it to one of the following: 'feedEnricher', 'campaignEvaluator', or 'eventActuator'.")
        exit(1)

    if mode not in valid_modes:
        print(f"Error: Invalid APP_MODE '{mode}'. Please choose from the following options: {', '.join(valid_modes)}.")
        exit(1)
    # Set the mode as an environment variable
    os.environ["APP_MODE"] = args.mode

    # Start the application using uvicorn
    subprocess.run(["uvicorn", "app.main:app", "--reload", "--host", "0.0.0.0", "--port", f"{port}"],)