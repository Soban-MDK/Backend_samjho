import click
from tasks import full_pipeline

@click.command()
def run_pipeline():
    """Manually triggers the full pipeline."""
    result = full_pipeline.delay()
    print(f"Pipeline execution started... Task ID: {result.id}")

if __name__ == "__main__":
    run_pipeline()