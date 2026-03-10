import { NavLink, Outlet } from 'react-router-dom'
import './Layout.css'

const NAV_ITEMS = [
  { to: '/', label: 'Dashboard', desc: 'Overview & KPIs' },
  { to: '/map', label: 'Site Map', desc: 'All intersections' },
  { to: '/clusters', label: 'Traffic Profiles', desc: 'Behaviour clusters' },
  { to: '/correlations', label: 'Linked Sites', desc: 'Origin-destination' },
  { to: '/models', label: 'Forecast Models', desc: 'Accuracy & metrics' },
]

export default function Layout() {
  return (
    <div className="layout">
      <nav className="sidebar">
        <div className="sidebar-brand">
          <h1>Flowcast</h1>
          <span className="sidebar-subtitle">Traffic Forecasting Platform</span>
        </div>
        <ul className="sidebar-nav">
          {NAV_ITEMS.map(({ to, label, desc }) => (
            <li key={to}>
              <NavLink to={to} end={to === '/'} className={({ isActive }) => isActive ? 'active' : ''}>
                <span className="nav-label">{label}</span>
                <span className="nav-desc">{desc}</span>
              </NavLink>
            </li>
          ))}
        </ul>
      </nav>
      <main className="content">
        <Outlet />
      </main>
    </div>
  )
}
