async function api(path) {
  const r = await fetch(path);
  if (!r.ok) throw "API error";
  return r.json();
}
