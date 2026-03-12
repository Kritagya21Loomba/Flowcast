// Thin fetch wrapper — works with both live API and static JSON files

const IS_STATIC = import.meta.env.VITE_STATIC === 'true'
const BASE_URL = import.meta.env.BASE_URL || '/'
const BASE = IS_STATIC ? `${BASE_URL}data` : '/api'

async function fetchJson<T>(path: string): Promise<T> {
  let url: string
  if (IS_STATIC) {
    // Strip query params and add .json extension
    const clean = path.split('?')[0]
    url = `${BASE}${clean}.json`
  } else {
    url = `${BASE}${path}`
  }
  const res = await fetch(url)
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${res.statusText}`)
  }
  return res.json()
}

export default fetchJson
