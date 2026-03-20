import { readFileSync } from "fs";
import { join } from "path";

export default function handler(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const filePath = join(process.cwd(), "public", "data", "brands.json");
    const data = JSON.parse(readFileSync(filePath, "utf-8"));

    let result = data;
    if (req.query.energy_type) {
      result = result.filter(d => d.energy_type === req.query.energy_type.toUpperCase());
    }

    res.setHeader("Cache-Control", "public, s-maxage=3600, stale-while-revalidate=86400");
    return res.status(200).json({ total: result.length, data: result });
  } catch (err) {
    return res.status(500).json({ error: "Internal server error" });
  }
}
