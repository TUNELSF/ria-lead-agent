'use client';

import React, { useEffect, useMemo, useState } from 'react';

type Lead = {
  id: string;
  firm: string;
  priority: 'high' | 'medium';
  signal_type: string;
  signal_category: string;
  trigger: string;
  why_now: string;
  source: string;
  source_label?: string;
  source_date?: string | null;
  evidence: string;
  hook: string;
  contacts: string[];
  status: string;
  firm_profile?: {
    aum?: number | null;
    aum_bucket?: string;
    growth_1y?: number | null;
    asset_focus?: string[];
    product_types?: string[];
    bd_affiliated?: boolean | null;
    independence?: string;
  };
};

const mockLeads: Lead[] = [
  {
    id: '1',
    firm: 'Morgan Stanley',
    priority: 'medium',
    signal_type: 'etf_activity',
    signal_category: 'portfolio',
    trigger: 'Portfolio activity involving crypto-related instruments detected in recent media',
    why_now:
      'This is a recent portfolio, ETF, demand, or adjacent alternatives signal that may indicate growing relevance of digital assets.',
    source: 'https://finance.yahoo.com/markets/crypto/articles/morgan-stanley-bitcoin-etf-goes-220824956.html',
    source_label: 'Yahoo Finance',
    source_date: '2026-04-08',
    evidence:
      'Morgan Stanley’s Bitcoin ETF Goes Live With Massive Inflow. MSBT debuts as the cheapest spot Bitcoin ETF, drawing strong early demand.',
    hook:
      'Saw the recent ETF activity — curious whether digital assets are becoming more relevant in portfolio construction conversations.',
    contacts: ['Chief Investment Officer', 'Managing Partner'],
    status: 'new',
    firm_profile: {
      aum_bucket: 'unknown',
      asset_focus: ['multi-asset'],
      product_types: ['ETF'],
      independence: 'bd_affiliated'
    }
  },
  {
    id: '2',
    firm: 'Two Prime Inc.',
    priority: 'high',
    signal_type: 'product_launch',
    signal_category: 'offering',
    trigger: 'Explicit crypto-related language found in recent media coverage',
    why_now:
      'This is recent, explicit crypto or digital-asset language tied to a specific media source.',
    source:
      'https://www.businesswire.com/news/home/20260116053866/en/Digital-Wealth-Partners-Chooses-Two-Prime-to-Manage-$250-Million-in-Bitcoin-Holdings',
    source_label: 'Business Wire',
    source_date: '2026-01-15',
    evidence:
      'Digital Wealth Partners chooses Two Prime to manage $250 million in Bitcoin holdings.',
    hook:
      'Saw the recent Bitcoin-mandate announcement — curious how you are thinking about institutionalizing digital-asset exposure for clients.',
    contacts: ['Christina Strauss', 'Chief Investment Officer'],
    status: 'reviewed',
    firm_profile: {
      aum_bucket: 'unknown',
      asset_focus: ['digital assets'],
      product_types: ['SMA'],
      independence: 'independent'
    }
  },
  {
    id: '3',
    firm: 'Example Independent RIA',
    priority: 'high',
    signal_type: 'demand_signal',
    signal_category: 'market_demand',
    trigger: 'Advisor demand for digital assets discussed in recent coverage',
    why_now:
      'Demand-based signals often indicate timing for outreach before product decisions are finalized.',
    source: 'https://example.com/news/digital-asset-demand',
    source_label: 'Example Media',
    source_date: '2026-04-12',
    evidence:
      'Leadership noted rising advisor demand for digital asset access among HNW clients.',
    hook:
      'Saw the advisor-demand commentary — curious how you are framing digital asset access internally as client interest picks up.',
    contacts: ['Chief Executive Officer', 'Head of Investments'],
    status: 'outreach_ready',
    firm_profile: {
      aum_bucket: '1B_plus',
      asset_focus: ['alternatives', 'wealth management'],
      product_types: ['UMA', 'ETF'],
      independence: 'independent'
    }
  }
];

function formatDate(dateStr?: string | null) {
  if (!dateStr) return 'Unknown date';
  try {
    return new Date(dateStr).toLocaleDateString();
  } catch {
    return dateStr;
  }
}

function priorityStyles(priority: string) {
  if (priority === 'high') {
    return {
      background: '#fee2e2',
      color: '#991b1b'
    };
  }

  return {
    background: '#fef3c7',
    color: '#92400e'
  };
}

function statusStyles(status: string) {
  if (status === 'reviewed') {
    return { background: '#ede9fe', color: '#5b21b6' };
  }
  if (status === 'outreach_ready') {
    return { background: '#dcfce7', color: '#166534' };
  }
  if (status === 'archived') {
    return { background: '#e2e8f0', color: '#334155' };
  }
  return { background: '#dbeafe', color: '#1d4ed8' };
}

export default function Page() {
  const [leads, setLeads] = useState<Lead[]>([]);
  const [search, setSearch] = useState('');
  const [priority, setPriority] = useState('all');
  const [status, setStatus] = useState('all');
  const [dataSource, setDataSource] = useState('mock data fallback');

  useEffect(() => {
    async function loadLeads() {
      try {
        const res = await fetch('/leads.json', { cache: 'no-store' });
        if (res.ok) {
          const data = await res.json();
          if (Array.isArray(data)) {
            setLeads(data);
            setDataSource('live leads.json');
            return;
          }
        }
      } catch {}

      setLeads(mockLeads);
    }

    loadLeads();
  }, []);

  const filtered = useMemo(() => {
    return leads.filter((lead) => {
      const searchOk = search
        ? lead.firm.toLowerCase().includes(search.toLowerCase())
        : true;

      const priorityOk = priority === 'all' ? true : lead.priority === priority;
      const statusOk = status === 'all' ? true : lead.status === status;

      return searchOk && priorityOk && statusOk;
    });
  }, [leads, search, priority, status]);

  return (
    <main
      style={{
        background: '#f8fafc',
        minHeight: '100vh',
        padding: 24,
        fontFamily: 'Arial, sans-serif',
        color: '#0f172a'
      }}
    >
      <div style={{ maxWidth: 1100, margin: '0 auto' }}>
        <div style={{ marginBottom: 24 }}>
          <div
            style={{
              display: 'inline-block',
              fontSize: 12,
              color: '#475569',
              marginBottom: 8,
              background: '#ffffff',
              border: '1px solid #e2e8f0',
              borderRadius: 999,
              padding: '6px 10px'
            }}
          >
            MVP dashboard • source: {dataSource}
          </div>

          <h1 style={{ fontSize: 40, margin: '8px 0' }}>RIA Signal Dashboard</h1>
          <p style={{ color: '#475569', lineHeight: 1.6, maxWidth: 800 }}>
            Review crypto and alternatives intent signals, filter them, and prepare outreach.
            The data structure is already ready for future filters like AUM, AUM growth,
            asset class, product type, ADV updates, people changes, and BD affiliation.
          </p>
        </div>

        <div
          style={{
            background: '#ffffff',
            border: '1px solid #e2e8f0',
            borderRadius: 16,
            padding: 16,
            marginBottom: 24
          }}
        >
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: '2fr 1fr 1fr',
              gap: 12
            }}
          >
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search firm"
              style={{
                padding: 12,
                borderRadius: 12,
                border: '1px solid #cbd5e1'
              }}
            />

            <select
              value={priority}
              onChange={(e) => setPriority(e.target.value)}
              style={{
                padding: 12,
                borderRadius: 12,
                border: '1px solid #cbd5e1'
              }}
            >
              <option value="all">All priorities</option>
              <option value="high">High</option>
              <option value="medium">Medium</option>
            </select>

            <select
              value={status}
              onChange={(e) => setStatus(e.target.value)}
              style={{
                padding: 12,
                borderRadius: 12,
                border: '1px solid #cbd5e1'
              }}
            >
              <option value="all">All statuses</option>
              <option value="new">New</option>
              <option value="reviewed">Reviewed</option>
              <option value="outreach_ready">Outreach ready</option>
              <option value="archived">Archived</option>
            </select>
          </div>

          <div style={{ marginTop: 12, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            <span
              style={{
                border: '1px solid #cbd5e1',
                borderRadius: 999,
                padding: '6px 10px',
                fontSize: 12,
                color: '#475569'
              }}
            >
              {filtered.length} leads
            </span>
            <span
              style={{
                border: '1px solid #cbd5e1',
                borderRadius: 999,
                padding: '6px 10px',
                fontSize: 12,
                color: '#475569'
              }}
            >
              future filters: AUM / AUM growth / asset class / product type / BD affiliation
            </span>
          </div>
        </div>

        <div style={{ display: 'grid', gap: 16 }}>
          {filtered.length === 0 ? (
            <div
              style={{
                background: '#ffffff',
                padding: 24,
                borderRadius: 16,
                border: '1px dashed #cbd5e1',
                color: '#64748b'
              }}
            >
              No leads match your filters.
            </div>
          ) : (
            filtered.map((lead) => (
              <div
                key={lead.id}
                style={{
                  background: '#ffffff',
                  padding: 20,
                  borderRadius: 16,
                  border: '1px solid #e2e8f0'
                }}
              >
                <div
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    gap: 16,
                    alignItems: 'flex-start'
                  }}
                >
                  <div>
                    <div style={{ marginBottom: 8, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                      <span
                        style={{
                          ...priorityStyles(lead.priority),
                          display: 'inline-block',
                          padding: '4px 10px',
                          borderRadius: 999,
                          fontSize: 12,
                          fontWeight: 700
                        }}
                      >
                        {lead.priority.toUpperCase()}
                      </span>

                      <span
                        style={{
                          ...statusStyles(lead.status),
                          display: 'inline-block',
                          padding: '4px 10px',
                          borderRadius: 999,
                          fontSize: 12,
                          fontWeight: 700
                        }}
                      >
                        {lead.status.replace('_', ' ')}
                      </span>

                      <span style={{ fontSize: 12, color: '#64748b', alignSelf: 'center' }}>
                        {lead.signal_type}
                      </span>
                    </div>

                    <h2 style={{ margin: 0, fontSize: 26 }}>{lead.firm}</h2>

                    <div style={{ fontSize: 13, color: '#64748b', marginTop: 6 }}>
                      {lead.source_label || 'Source'} • {formatDate(lead.source_date)}
                    </div>
                  </div>

                  <a
                    href={lead.source}
                    target="_blank"
                    rel="noreferrer"
                    style={{
                      textDecoration: 'none',
                      padding: '10px 14px',
                      borderRadius: 12,
                      border: '1px solid #cbd5e1',
                      color: '#0f172a',
                      whiteSpace: 'nowrap'
                    }}
                  >
                    Open source
                  </a>
                </div>

                <div style={{ marginTop: 18, display: 'grid', gap: 14 }}>
                  <div>
                    <div style={{ fontSize: 12, textTransform: 'uppercase', color: '#64748b', marginBottom: 4 }}>
                      Trigger
                    </div>
                    <div>{lead.trigger}</div>
                  </div>

                  <div>
                    <div style={{ fontSize: 12, textTransform: 'uppercase', color: '#64748b', marginBottom: 4 }}>
                      Why now
                    </div>
                    <div>{lead.why_now}</div>
                  </div>

                  <div>
                    <div style={{ fontSize: 12, textTransform: 'uppercase', color: '#64748b', marginBottom: 4 }}>
                      Evidence
                    </div>
                    <div>{lead.evidence}</div>
                  </div>

                  <div>
                    <div style={{ fontSize: 12, textTransform: 'uppercase', color: '#64748b', marginBottom: 4 }}>
                      Suggested hook
                    </div>
                    <div style={{ fontWeight: 600 }}>{lead.hook}</div>
                  </div>

                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
                    <div>
                      <div style={{ fontSize: 12, textTransform: 'uppercase', color: '#64748b', marginBottom: 6 }}>
                        Contacts
                      </div>
                      <ul style={{ margin: 0, paddingLeft: 18 }}>
                        {lead.contacts.map((contact) => (
                          <li key={contact} style={{ marginBottom: 4 }}>
                            {contact}
                          </li>
                        ))}
                      </ul>
                    </div>

                    <div>
                      <div style={{ fontSize: 12, textTransform: 'uppercase', color: '#64748b', marginBottom: 6 }}>
                        Firm profile
                      </div>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                        <span
                          style={{
                            border: '1px solid #cbd5e1',
                            borderRadius: 999,
                            padding: '6px 10px',
                            fontSize: 12
                          }}
                        >
                          AUM: {lead.firm_profile?.aum_bucket || 'unknown'}
                        </span>

                        {(lead.firm_profile?.product_types || []).map((type) => (
                          <span
                            key={type}
                            style={{
                              border: '1px solid #cbd5e1',
                              borderRadius: 999,
                              padding: '6px 10px',
                              fontSize: 12
                            }}
                          >
                            {type}
                          </span>
                        ))}

                        {(lead.firm_profile?.asset_focus || []).map((focus) => (
                          <span
                            key={focus}
                            style={{
                              border: '1px solid #cbd5e1',
                              borderRadius: 999,
                              padding: '6px 10px',
                              fontSize: 12
                            }}
                          >
                            {focus}
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>

                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    <button
                      style={{
                        border: 'none',
                        borderRadius: 12,
                        padding: '10px 14px',
                        background: '#0f172a',
                        color: '#ffffff',
                        cursor: 'pointer'
                      }}
                    >
                      Mark good
                    </button>

                    <button
                      style={{
                        border: '1px solid #cbd5e1',
                        borderRadius: 12,
                        padding: '10px 14px',
                        background: '#ffffff',
                        cursor: 'pointer'
                      }}
                    >
                      Outreach ready
                    </button>

                    <button
                      style={{
                        border: '1px solid #cbd5e1',
                        borderRadius: 12,
                        padding: '10px 14px',
                        background: '#ffffff',
                        cursor: 'pointer'
                      }}
                    >
                      Archive
                    </button>
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </main>
  );
}
