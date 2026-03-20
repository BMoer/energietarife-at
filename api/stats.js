import { readFileSync } from "fs";
import { join } from "path";

export default function handler(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const filePath = join(process.cwd(), "public", "data", "stats.json");
    const data = JSON.parse(readFileSync(filePath, "utf-8"));
    res.setHeader("Cache-Control", "public, s-maxage=3600, stale-while-revalidate=86400");
    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json({ error: "Internal server error" });
  }
}
