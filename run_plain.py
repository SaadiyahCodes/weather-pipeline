from pipeline.ingest import run_ingest
from pipeline.silver import load_bronze, build_silver, publish_silver
from pipeline.quality import assert_quality
from pipeline.gold import build_gold

run_ingest(offline=True)
df = build_silver(load_bronze())
assert_quality(df)
publish_silver(df)
print(build_gold())