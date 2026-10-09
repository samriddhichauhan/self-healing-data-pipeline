import { useEffect, useState } from "react";
import { Outlet, useLocation } from "react-router-dom";
import { useSettings } from "../../lib/settings";
import { findNav } from "./nav";
import { Sidebar } from "./Sidebar";
import { Topbar } from "./Topbar";

export function AppShell() {
  const [navOpen, setNavOpen] = useState(false);
  const { pathname } = useLocation();
  const { settings } = useSettings();
  const item = findNav(pathname);

  useEffect(() => setNavOpen(false), [pathname]);
  useEffect(() => { document.title = `${item.label} · SelfHeal AI`; }, [item.label]);

  return (
    <div className={`shell ${settings.sidebarCollapsed ? "collapsed" : ""}`}>
      <div className={`scrim ${navOpen ? "open" : ""}`} onClick={() => setNavOpen(false)} />
      <Sidebar open={navOpen} />
      <div className="main">
        <Topbar item={item} onMenu={() => setNavOpen(true)} />
        <main className="content" key={item.to}>
          <Outlet />
        </main>
      </div>
    </div>
  );
}
