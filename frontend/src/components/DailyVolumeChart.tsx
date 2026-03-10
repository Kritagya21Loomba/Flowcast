import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import type { DailyVolume } from '../api/types'

interface Props {
  data: DailyVolume[]
}

export default function DailyVolumeChart({ data }: Props) {
  const formatted = data.map((d) => ({
    ...d,
    date: d.date.slice(5), // MM-DD
  }))

  return (
    <ResponsiveContainer width="100%" height={180}>
      <AreaChart data={formatted} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
        <defs>
          <linearGradient id="volGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#60a5fa" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#60a5fa" stopOpacity={0} />
          </linearGradient>
        </defs>
        <XAxis dataKey="date" tick={{ fill: '#888', fontSize: 10 }} interval="preserveStartEnd" />
        <YAxis tick={{ fill: '#888', fontSize: 10 }} width={50} tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
        <Tooltip
          contentStyle={{ background: '#1a1d23', border: '1px solid #2a2d35', fontSize: '0.8rem' }}
          labelStyle={{ color: '#60a5fa' }}
          formatter={(value) => [Number(value).toLocaleString(), 'Volume']}
        />
        <Area type="monotone" dataKey="total_volume" stroke="#60a5fa" fill="url(#volGrad)" />
      </AreaChart>
    </ResponsiveContainer>
  )
}
