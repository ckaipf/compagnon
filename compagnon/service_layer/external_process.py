
import abc
import subprocess



class FailedExternalProcess(Exception):
    def __init__(self, returncode, stderr):
        self.returncode = returncode
        self.stderr = stderr.decode("utf-8").strip()
        print(stderr)
        self.message = stderr
        super().__init__(self.message)

class AbstractExternalProcess(abc.ABC):
    def __init__(self, cmd):
        self.cmd = cmd


    def run(self) -> str:
        self._run()
        self.handle_errors()
        return self.get_stdout()
        
    @abc.abstractmethod
    def _run(self):
        raise NotImplementedError
    
    @abc.abstractmethod
    def handle_errors(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_stdout(self) -> str:
        raise NotImplementedError


class SubprocessProcess(AbstractExternalProcess):
    
    def _run(self):
        self.process = subprocess.run(self.cmd, capture_output=True, shell=True, check=False)

    def handle_errors(self) -> None:
        try:
            if self.process.returncode != 0:
                raise FailedExternalProcess(self.process.returncode, self.process.stderr)
        except AttributeError as exc:
            raise AttributeError("The process has not yet been run") from exc
        
    def get_stdout(self) -> str:
        try:
            return self.process.stdout.decode("utf-8").strip()
        except AttributeError as exc:
                raise AttributeError("The process has not yet been run") from exc
        