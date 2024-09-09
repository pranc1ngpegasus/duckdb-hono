import path from "path";
import { serve } from "@hono/node-server";
import { Database } from "duckdb-async";
import { Hono } from "hono";
import { logger } from "hono/logger";

let db: Database;
const loadCSV = async (): Promise<void> => {
  try {
    db = await Database.create(":memory:");
    const csvPath = path.resolve(__dirname, "utf_ken_all.csv");
    await db.exec(`
      CREATE TABLE postal_code AS 
      SELECT * FROM read_csv_auto('${csvPath}', 
        delim = ',',
        header = false,
        columns = {
          local_gov_code: 'VARCHAR',
          old_zip_code: 'VARCHAR',
          zip_code: 'VARCHAR',
          prefecture_kana: 'VARCHAR',
          city_kana: 'VARCHAR',
          town_kana: 'VARCHAR',
          prefecture: 'VARCHAR',
          city: 'VARCHAR',
          town: 'VARCHAR',
          multiple_zip_codes: 'BOOLEAN',
          koaza_banchi: 'BOOLEAN',
          has_chome: 'BOOLEAN',
          multiple_towns: 'BOOLEAN',
          update_status: 'INTEGER',
          update_reason: 'INTEGER'
        }
      )
    `);
  } catch (err) {
    console.error(err);
    throw err;
  }
};

const app = new Hono();
app.use(logger());

app.get("/healthcheck", (c) => c.text("ok"));
app.get("/postal_code", async (c) => {
  try {
    const limit = c.req.query("limit") || "10";
    const records = await db.all("SELECT * FROM postal_code LIMIT ?", [limit]);
    return c.json(records);
  } catch (err) {
    console.error(err);
    return c.json({ error: err }, 500);
  }
});
app.get("/postal_code/:zip_code", async (c) => {
  try {
    const zipCode = c.req.param("zip_code");
    const record = await db.all("SELECT * FROM postal_code WHERE zip_code = ? LIMIT 1", [zipCode]);
    if (record.length === 0) {
      return c.json({ error: "Not found" }, 404);
    }
    return c.json(record[0]);
  } catch (err) {
    console.error(err);
    return c.json({ error: err }, 500);
  }
});

const start = async () => {
  try {
    await loadCSV();
    serve(
      {
        fetch: app.fetch,
        port: process.env.PORT || 3000,
      },
      (info) => {
        console.log(`Server is running on http://localhost:${info.port}/`);
      },
    );
  } catch (err) {
    console.error(err);
  }
};

start();
