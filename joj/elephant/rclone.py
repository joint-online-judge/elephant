"""
A Python wrapper for rclone.
"""
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pylint: disable=W0102,W0703,C0103


import subprocess
import tempfile
from typing import Any, Dict, List

from loguru import logger


class RClone:
    """
    Wrapper class for rclone.
    """

    def __init__(self, cfg: str) -> None:
        self.cfg = cfg.replace("\\n", "\n")
        logger.debug("rclone config: ~{}~", self.cfg)

    def _execute(self, command_with_args: List[str]) -> Dict[str, Any]:
        """
        Execute the given `command_with_args` using Popen
        Args:
            - command_with_args (list) : An array with the command to execute,
                                         and its arguments. Each argument is given
                                         as a new element in the list.
        """
        logger.debug("Invoking : {}", " ".join(command_with_args))
        try:
            with subprocess.Popen(
                command_with_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            ) as proc:
                (out, err) = proc.communicate()

                # out = proc.stdout.read()
                # err = proc.stderr.read()

                logger.debug(out)
                if err:
                    logger.warning(err.decode("utf-8").replace("\\n", "\n"))

                return {"code": proc.returncode, "out": out, "error": err}
        except FileNotFoundError as not_found_e:
            logger.error("Executable not found. {}", not_found_e)
            return {"code": -20, "error": not_found_e}
        except Exception as generic_e:
            logger.exception("Error running command. Reason: {}", generic_e)
            return {"code": -30, "error": generic_e}

    def run_cmd(self, command: str, extra_args: List[str] = []) -> Dict[str, Any]:
        """
        Execute rclone command
        Args:
            - command (string): the rclone command to execute.
            - extra_args (list): extra arguments to be passed to the rclone command
        """
        # save the configuration in a temporary file
        with tempfile.NamedTemporaryFile(mode="wt", delete=True) as cfg_file:
            # cfg_file is automatically cleaned up by python
            cfg_file.write(self.cfg)
            cfg_file.flush()

            command_with_args = ["rclone", command, "--config", cfg_file.name]
            command_with_args += extra_args
            command_with_args += ["--s3-force-path-style"]
            command_result = self._execute(command_with_args)
            cfg_file.close()
            return command_result

    def copy(self, source: str, dest: str, flags: List[str] = []) -> Dict[str, Any]:
        """
        Executes: rclone copy source:path dest:path [flags]
        Args:
        - source (string): A string "source:path"
        - dest (string): A string "dest:path"
        - flags (list): Extra flags as per `rclone copy --help` flags.
        """
        return self.run_cmd(command="copy", extra_args=[source] + [dest] + flags)

    def sync(self, source: str, dest: str, flags: List[str] = []) -> Dict[str, Any]:
        """
        Executes: rclone sync source:path dest:path [flags]
        Args:
        - source (string): A string "source:path"
        - dest (string): A string "dest:path"
        - flags (list): Extra flags as per `rclone sync --help` flags.
        """
        return self.run_cmd(command="sync", extra_args=[source] + [dest] + flags)

    def listremotes(self, flags: List[str] = []) -> Dict[str, Any]:
        """
        Executes: rclone listremotes [flags]
        Args:
        - flags (list): Extra flags as per `rclone listremotes --help` flags.
        """
        return self.run_cmd(command="listremotes", extra_args=flags)

    def ls(self, dest: str, flags: List[str] = []) -> Dict[str, Any]:
        """
        Executes: rclone ls remote:path [flags]
        Args:
        - dest (string): A string "remote:path" representing the location to list.
        """
        return self.run_cmd(command="ls", extra_args=[dest] + flags)

    def lsjson(self, dest: str, flags: List[str] = []) -> Dict[str, Any]:
        """
        Executes: rclone lsjson remote:path [flags]
        Args:
        - dest (string): A string "remote:path" representing the location to list.
        """
        return self.run_cmd(command="lsjson", extra_args=[dest] + flags)

    def delete(self, dest: str, flags: List[str] = []) -> Dict[str, Any]:
        """
        Executes: rclone delete remote:path
        Args:
        - dest (string): A string "remote:path" representing the location to delete.
        """
        return self.run_cmd(command="delete", extra_args=[dest] + flags)


def with_config(cfg: str) -> RClone:
    """
    Configure a new RClone instance.
    """
    inst = RClone(cfg=cfg)
    return inst
