from gptzero import GPT2PPL
import polars as pl


# model = GPT2PPL(device="cuda:0")

def load_snipped(name: str) -> pl.DataFrame:
    pass


RAW_SAMPLE = (pl
              .read_parquet("/mnt/Fast2T/data/ai-training-set/human/wiki_parquet_output/wiki_00000.parquet")
              .sample(500)
              .select("text"))

print(RAW_SAMPLE)

# print(model("""
# Oder wir bauen zuerst ein 2d minecraft clone als background rein, wo der start screen aussieht wie ein normales hintergrund bild, weil white wool oder so. Aber dann wenn man das brickt kann man in minecraft durch gehen. Und wenn man den enderdragon besigt kommt die andere seite
# Das sollte in 30min machbar sein /s
# """))
