import {
  ComposedChart,
  Area,
  Line,
  Scatter,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import type { ForecastPoint } from '../api/types'

interface Props {
  forecasts: ForecastPoint[]
}

export default function ForecastChart({ forecasts }: Props) {
  const data = forecasts.map((f) => ({
    date: f.forecast_date.slice(5),
    predicted: Math.round(f.predicted_volume),
    lower: f.prediction_lower ? Math.round(f.prediction_lower) : undefined,
    upper: f.prediction_upper ? Math.round(f.prediction_upper) : undefined,
    actual: f.actual_volume ?? undefined,
    // For the area band
    band: f.prediction_lower && f.prediction_upper
      ? [Math.round(f.prediction_lower), Math.round(f.prediction_upper)]
      : undefined,
  }))

  return (
    <ResponsiveContainer width="100%" height={180}>
      <ComposedChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
        <XAxis dataKey="date" tick={{ fill: '#888', fontSize: 10 }} />
        <YAxis tick={{ fill: '#888', fontSize: 10 }} width={50} tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
        <Tooltip
          contentStyle={{ background: '#1a1d23', border: '1px solid #2a2d35', fontSize: '0.8rem' }}
          labelStyle={{ color: '#60a5fa' }}
          labelFormatter={(label) => `Date: ${label}`}
          formatter={(value, name) => [Number(value).toLocaleString(), name === 'predicted' ? 'Predicted' : name === 'actual' ? 'Actual' : String(name)]}
        />
        {/* Confidence band */}
        <Area dataKey="upper" stroke="none" fill="#60a5fa" fillOpacity={0.1} />
        <Area dataKey="lower" stroke="none" fill="#111318" fillOpacity={1} />
        {/* Predicted line */}
        <Line type="monotone" dataKey="predicted" stroke="#60a5fa" strokeWidth={2} dot={false} />
        {/* Actual points */}
        <Scatter dataKey="actual" fill="#34d399" />
      </ComposedChart>
    </ResponsiveContainer>
  )
}
