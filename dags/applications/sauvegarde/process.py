import subprocess
import json


def convert_str_to_ascii_str(str: str):
    import unicodedata

    normalized = unicodedata.normalize("NFD", str)
    ascii_str = "".join(
        [char for char in normalized if unicodedata.category(char) != "Mn"]
    )
    ascii_str = (
        ascii_str.replace("'", "_").replace(" ", "_").replace("[", "").replace("]", "")
    )
    return ascii_str


def get_pod_name(namespace: str, pod_label: str) -> str:
    """Get the first pod matching the label selector."""
    cmd = f"kubectl get pods -n {namespace} -l {pod_label} -o json"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        raise ValueError("Error getting pod name:", result.stderr)

    pods = json.loads(result.stdout)
    if pods["items"]:
        return pods["items"][0]["metadata"]["name"]

    raise ValueError("No matching pods found")


def execute_command_in_pod(namespace: str, pod_name: str, sh_command: str) -> bool:
    """Run the export command inside the pod."""
    cmd = f"kubectl exec -n {namespace} {pod_name} -- {sh_command}"
    result = subprocess.run(cmd, shell=False, capture_output=True, text=True)

    if result.returncode != 0:
        raise ValueError("Error executing command in pod:", result.stderr)

    print("Command executed successfully")
    return True


def copy_file_from_pod(
    namespace: str, pod_name: str, pod_filepath: str, local_filepath: str
):
    """Copy the JSON file from the pod to local system."""
    cmd = f"kubectl cp {namespace}/{pod_name}:{pod_filepath} {local_filepath}"
    result = subprocess.run(cmd, shell=False, capture_output=True, text=True)

    if result.returncode != 0:
        raise ValueError("Error copying file from pod:", result.stderr)

    print(f"File copied successfully to {local_filepath}")
