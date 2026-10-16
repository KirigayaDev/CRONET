"""Run only disposable D5 containers; no repository .env or stored credentials."""
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import uuid


def main():
    root = Path(__file__).resolve().parents[5]
    compose_file = Path(__file__).with_name('compose.clickhouse-test.yml')
    project = f'cronet-d5-{uuid.uuid4().hex[:12]}'
    env = os.environ.copy()
    for name in ('ADMIN_PASSWORD', 'READER_PASSWORD', 'CLICKHOUSE_PASSWORD'):
        env[f'D5_TEST_{name}'] = secrets.token_hex(32)
    env['D5_TEST_READER_USER'] = f'd5_reader_{uuid.uuid4().hex[:16]}'
    with tempfile.TemporaryDirectory(prefix='cronet-d5-') as directory:
        safe_env = Path(directory) / 'empty.env'
        safe_env.touch()
        compose = ['docker', 'compose', '-p', project, '-f', str(compose_file),
                   '--env-file', str(safe_env)]
        try:
            subprocess.run(compose + ['up', '-d', '--build', '--wait', 'rust'],
                           cwd=root, env=env, check=True)
            subprocess.run(compose + ['exec', '-T', 'rust', 'cargo', 'test',
                           '--locked', '-p', 'cmdb-service', '--test',
                           'clickhouse_dictionaries', '--', '--ignored', '--test-threads=1'],
                           cwd=root, env=env, check=True)
        except subprocess.CalledProcessError:
            logs = subprocess.run(compose + ['logs', '--no-color', '--tail', '80',
                                  'clickhouse', 'database_migrations'],
                                  cwd=root, env=env, capture_output=True, text=True)
            output = logs.stdout + logs.stderr
            for name, value in env.items():
                if name.startswith('D5_TEST_') and value:
                    output = output.replace(value, '[redacted test value]')
            print(output, flush=True)
            raise
        finally:
            subprocess.run(compose + ['down', '--volumes'], cwd=root, env=env, check=True)


if __name__ == '__main__':
    main()
