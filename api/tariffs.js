import { readFileSync } from "fs";
import { join } from "path";

function loadData(filename) {
  const filePath = join(process.cwd(), "public", "data", filename);
  return JSON.parse(readFileSync(filePath, "utf-8"));
}

function filterData(data, query) {
  let result = data;

  if (query.energy_type) {
    result = result.filter(d => d.energy_type === query.energy_type.toUpperCase());
  }
  if (query.brand) {
    const brand = query.brand.toLowerCase();
    result = result.filter(d => d.brand_name.toLowerCase().includes(brand));
  }
  if (query.from) {
    result = result.filter(d => d.product_validity_from >= query.from);
  }
  if (query.to) {
    result = result.filter(d => d.product_validity_from <= query.to);
  }
  if (query.max_price) {
    const max = parseFloat(query.max_price);
    result = result.filter(d => d.energy_rate_ct_kwh <= max);
  }
  if (query.min_price) {
    const min = parseFloat(query.min_price);
    result = result.filter(d => d.energy_rate_ct_kwh >= min);
  }

  const limit = Math.min(parseInt(query.limit) || 1000, 5000);
  const offset = parseInt(query.offset) || 0;
  const total = result.length;
  result = result.slice(offset, offset + limit);

  return { total, limit, offset, data: result };
}

export default function handler(req, res) {
  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const data = loadData("historical.json");
    const result = filterData(data, req.query);
    res.setHeader("Cache-Control", "public, s-maxage=3600, stale-while-revalidate=86400");
    return res.status(200).json(result);
  } catch (err) {
    return res.status(500).json({ error: "Internal server error" });
  }
}
