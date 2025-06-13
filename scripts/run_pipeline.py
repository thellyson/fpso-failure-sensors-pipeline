import subprocess
import sys
from pathlib import Path

# Lista de scripts a serem executados em sequência
default_scripts = [
    "ingest_equipments.py",
    "ingest_equipment_sensors.py",
    "ingest_failures.py",
    "silver_equipment_failure_sensors.py",
    "gold_equipment_failures_summary.py"
]


def main(scripts=None):
    scripts = scripts or default_scripts
    base_dir = Path(__file__).resolve().parent

    for script_name in scripts:
        script_path = base_dir / script_name
        if not script_path.exists():
            print(f"[ERROR] Script não encontrado: {script_path}")
            sys.exit(1)

        print(f"\n=== Executando: {script_name} ===")
        # Executa com o mesmo interpretador usado neste script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(base_dir)
        )

        if result.returncode != 0:
            print(f"[ERROR] O script {script_name} falhou com código {result.returncode}. Interrompendo pipeline.")
            sys.exit(result.returncode)

        print(f"[OK] {script_name} concluído com sucesso.")

    print("\nPipeline finalizado com sucesso.")


if __name__ == '__main__':
    main()
