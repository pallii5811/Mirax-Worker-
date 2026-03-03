"use client";

import { AnimatePresence, motion } from "framer-motion";
import {
  ArrowDown,
  ArrowUp,
  ExternalLink,
  FileDown,
  MessageCircle,
  Phone,
  Search,
  Rocket,
} from "lucide-react";
import * as React from "react";

import type { BusinessResult, JobStatus, TechnicalAuditResult } from "@/lib/api";
import {
  BACKEND_URL,
  fetchJob,
  fetchResults,
  fetchTechnicalAudit,
  startJob,
} from "@/lib/api";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

const zone_database: Record<string, string[]> = {
  Milano: [
    "Tutta la città",
    "Centro Storico",
    "Brera",
    "Porta Nuova",
    "Isola",
    "CityLife",
    "Navigli",
    "Porta Romana",
    "Porta Venezia",
    "Lambrate",
    "NoLo",
    "Bicocca",
    "San Siro",
    "Città Studi",
    "Ticinese",
    "Sempione",
  ],
  Roma: [
    "Tutta la città",
    "Centro Storico",
    "Trastevere",
    "Prati",
    "Parioli",
    "EUR",
    "Testaccio",
    "San Lorenzo",
    "Pigneto",
    "Garbatella",
    "Monteverde",
    "Flaminio",
    "Ostiense",
    "Monti",
    "Trieste",
    "Nomentano",
  ],
  Torino: [
    "Tutta la città",
    "Centro",
    "Crocetta",
    "San Salvario",
    "Vanchiglia",
    "Cit Turin",
    "Santa Rita",
    "Lingotto",
    "Gran Madre",
    "Aurora",
    "San Paolo",
  ],
  Napoli: [
    "Tutta la città",
    "Chiaia",
    "Vomero",
    "Posillipo",
    "Centro Storico",
    "Rione Sanità",
    "Fuorigrotta",
    "Arenella",
    "San Ferdinando",
    "Porto",
  ],
  Bologna: [
    "Tutta la città",
    "Centro Storico",
    "Bolognina",
    "Saragozza",
    "San Donato",
    "Murri",
    "Colli",
    "Santo Stefano",
    "Porto",
  ],
  Firenze: [
    "Tutta la città",
    "Centro Storico",
    "Oltrarno",
    "Campo di Marte",
    "Rifredi",
    "Novoli",
    "Santa Croce",
    "Santo Spirito",
  ],
  Genova: [
    "Tutta la città",
    "Centro",
    "Porto Antico",
    "Albaro",
    "Castelletto",
    "Nervi",
    "Pegli",
    "Sampierdarena",
    "Foce",
  ],
  Venezia: [
    "Tutta la città",
    "San Marco",
    "Cannaregio",
    "Castello",
    "Dorsoduro",
    "Santa Croce",
    "San Polo",
    "Giudecca",
    "Mestre Centro",
    "Lido",
  ],
  Verona: [
    "Tutta la città",
    "Centro Storico",
    "Borgo Trento",
    "Borgo Venezia",
    "Borgo Roma",
    "San Zeno",
    "Veronetta",
  ],
  Bari: [
    "Tutta la città",
    "Bari Vecchia",
    "Murat",
    "Poggiofranco",
    "Carrassi",
    "San Pasquale",
    "Madonnella",
  ],
  Palermo: [
    "Tutta la città",
    "Centro Storico",
    "Politeama",
    "Libertà",
    "Kalsa",
    "Mondello",
    "Zisa",
  ],
  Catania: [
    "Tutta la città",
    "Centro Storico",
    "Corso Italia",
    "Lungomare",
    "Borgo-Sanzio",
    "Cibali",
  ],
  Padova: ["Tutta la città", "Centro", "Arcella", "Guizza", "Portello", "Stanga", "Santa Croce"],
  Trieste: [
    "Tutta la città",
    "Borgo Teresiano",
    "San Giusto",
    "Barcola",
    "Roiano",
    "Città Vecchia",
  ],
  Brescia: [
    "Tutta la città",
    "Centro Storico",
    "Brescia Due",
    "Mompiano",
    "Lamarmora",
    "Borgo Trento",
  ],
  Bergamo: [
    "Tutta la città",
    "Città Alta",
    "Città Bassa",
    "Borgo Palazzo",
    "Redona",
    "Loreto",
  ],
  Salerno: [
    "Tutta la città",
    "Centro Storico",
    "Lungomare",
    "Pastena",
    "Torrione",
    "Carmine",
  ],
  Monza: ["Tutta la città", "Centro", "Parco", "San Biagio", "Triante", "San Fruttuoso"],
  Parma: ["Tutta la città", "Centro Storico", "Oltretorrente", "Cittadella", "San Lazzaro"],
  Modena: ["Tutta la città", "Centro Storico", "Crocetta", "Buon Pastore", "San Faustino"],
};

function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = React.useState(value);
  React.useEffect(() => {
    const t = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(t);
  }, [value, delayMs]);
  return debounced;
}

function buildPitchEmail(row: BusinessResult) {
  const name = row.business_name;

  if (row.website_status === "MISSING_WEBSITE") {
    return {
      subject: `Proposta rapida: aumentiamo i clienti di ${name} (presenza online)`,
      body:
        `Ciao ${name},\n\n` +
        `ho trovato la tua attività su Google Maps ma non risulta un sito web ufficiale. Oggi questo significa perdere richieste e prenotazioni da chi cerca online e confronta alternative.\n\n` +
        `In 7-10 giorni possiamo realizzare una landing/sito veloce, ottimizzato per mobile e per la conversione, con tracking completo e contatti immediati (WhatsApp/call).\n\n` +
        `Se ti va, ti mando una bozza gratuita della struttura (sezioni + call-to-action) e una stima dei tempi.\n\n` +
        `Ti interessa che te la prepari?\n\n` +
        `—`,
    };
  }

  if (row.website_status === "HAS_WEBSITE" && !row.audit.has_facebook_pixel) {
    return {
      subject: `${name}: manca il Pixel FB (stai perdendo retargeting)`,
      body:
        `Ciao ${name},\n\n` +
        `ho fatto un audit rapido del tuo sito e non ho rilevato il Facebook Pixel attivo. Questo di solito significa che eventuali campagne ads non possono fare retargeting in modo efficace (chi visita il sito poi “sparisce”).\n\n` +
        `In pratica: stai pagando traffico, ma perdi la parte più profittevole (recupero visitatori, lookalike, ottimizzazione eventi).\n\n` +
        `Posso: installare Pixel + eventi (lead/chiamata/whatsapp), verificare conversioni e impostare una struttura retargeting base.\n\n` +
        `Ti va se ti mando un piano di intervento in 3 punti + costo fisso?\n\n` +
        `—`,
    };
  }

  return {
    subject: `Complimenti ${name}: audit positivo — proposta di crescita`,
    body:
      `Ciao ${name},\n\n` +
      `complimenti: dal controllo che ho fatto risulta una buona base (sito presente e tracking ok).\n\n` +
      `Se vuoi spingere ancora, posso proporti una gestione avanzata: contenuti + social + campagne con ottimizzazione e reporting mensile, per aumentare richieste e prenotazioni.\n\n` +
      `Ti va una call di 10 minuti per capire obiettivi e target?\n\n` +
      `—`,
  };
}

function buildAuditIssues(row: BusinessResult): string[] {
  const issues: string[] = [];
  if (row.website_status === "MISSING_WEBSITE") {
    issues.push("Sito Web: ASSENTE");
    return issues;
  }

  if (!row.audit.has_facebook_pixel) issues.push("Facebook Pixel: MANCANTE");
  if (!row.audit.has_gtm) issues.push("Google Tag Manager: MANCANTE");
  if (!row.audit.has_ssl) issues.push("SSL (HTTPS): NON ATTIVO");
  if (!row.audit.is_mobile_responsive) issues.push("Mobile Responsive: NON RILEVATO");
  return issues;
}

function cleanPhoneForWhatsApp(phone: string): string {
  let p = phone.trim();
  const hasExplicitPrefix = p.startsWith("+") || p.startsWith("00");
  p = p.replace(/^\+/, "");
  p = p.replace(/^00/, "");
  p = p.replace(/[^0-9]/g, "");

  // If it's a local Italian mobile (9/10 digits starting with 3) force country code 39.
  // Do not infer any other foreign prefixes.
  if (!hasExplicitPrefix && (p.length === 9 || p.length === 10) && p.startsWith("3")) {
    return "39" + p;
  }

  // If already includes 39, keep it (wa.me expects country code, without +)
  return p;
}

function isWhatsappEligible(phone?: string | null): boolean {
  if (!phone) return false;
  const raw = String(phone).trim();
  // Requirement: only if final number starts with +393
  if (raw.startsWith("+393")) return true;
  if (raw.startsWith("00393")) return true;
  return false;
}

function normalizePhoneForType(phone?: string | null): string {
  if (!phone) return "";
  let p = String(phone).trim();
  p = p.replace(/\s+/g, "");
  p = p.replace(/[-()]/g, "");
  // Keep digits only
  p = p.replace(/[^0-9]/g, "");
  if (p.startsWith("0039")) p = p.slice(4);
  if (p.startsWith("39") && p.length > 10) p = p.slice(2);
  return p;
}

function isMobilePhone(phone?: string | null): boolean {
  const p = normalizePhoneForType(phone);
  if (!p) return false;
  return p.startsWith("3");
}

function buildSocialSearchUrl(businessName: string, city: string): string {
  const q = `site:instagram.com OR site:facebook.com "${businessName} ${city}"`;
  return `https://www.google.com/search?q=${encodeURIComponent(q)}`;
}

function MarketingBadges({ row }: { row: BusinessResult }) {
  if (!hasWebsite(row)) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }

  const missingFb = row.website_status === "HAS_WEBSITE" && !row.audit.has_facebook_pixel;
  const missingGtm = row.website_status === "HAS_WEBSITE" && !row.audit.has_gtm;
  const missingInsta = Boolean(
    (row as any).instagram_missing ?? (row.audit as any).instagram_missing ?? row.audit.missing_instagram,
  );

  return (
    <div className="flex flex-wrap items-center gap-2">
      {missingFb ? (
        <Badge variant="danger">MISSING FB PIXEL</Badge>
      ) : (
        <Badge variant="success">FB PIXEL OK</Badge>
      )}
      {!row.audit.has_tiktok_pixel ? (
        <Badge variant="warning">No TikTok</Badge>
      ) : (
        <Badge variant="success">TikTok OK</Badge>
      )}
      {missingInsta ? (
        <span
          style={{
            background: "rgba(193, 53, 132, 0.2)",
            color: "#E1306C",
            border: "1px solid #E1306C",
            padding: "2px 6px",
            borderRadius: "4px",
            fontSize: "0.7rem",
            marginRight: "5px",
            fontWeight: "bold",
          }}
        >
          NO INSTA
        </span>
      ) : (
        <Badge variant="success">INSTA OK</Badge>
      )}
      {missingGtm ? (
        <Badge variant="warning">Missing GTM</Badge>
      ) : (
        <Badge variant="success">GTM OK</Badge>
      )}
    </div>
  );
}

function WebsiteCodeError({ row }: { row: BusinessResult }) {
  const status = row.website_http_status ?? null;
  const err = String(row.website_error ?? "").trim();

  const show = (typeof status === "number" && status >= 400) || err.length > 0;
  if (!show) return null;

  const label =
    typeof status === "number" && status >= 400
      ? `HTTP ${status}`
      : err || "Errore sito";

  return (
    <div className="mb-2 rounded-md border border-red-500/40 bg-red-500/10 px-2 py-1 text-[11px] font-medium text-red-400">
      ERRORE CODICE SITO: {label}
    </div>
  );
}

function WebsiteCodeErrorCell({ row, jobId }: { row: BusinessResult; jobId: string }) {
  const status = row.website_http_status ?? null;
  const err = String(row.website_error ?? "").trim();

  const show = (typeof status === "number" && status >= 400) || err.length > 0;
  if (!show) return <span className="text-xs text-muted-foreground">—</span>;

  const label =
    typeof status === "number" && status >= 400
      ? `HTTP ${status}`
      : err || "Errore";

  const line = row.website_error_line ?? null;
  const url = `${BACKEND_URL}/jobs/${jobId}/sites/${row.result_index}/html${
    line ? `?line=${encodeURIComponent(String(line))}` : ""
  }`;
  const hasHtml = Boolean(row.website_has_html);

  return (
    <div className="flex flex-col gap-1">
      <div className="rounded-md border border-red-500/40 bg-red-500/10 px-2 py-1 text-[11px] font-semibold text-red-400">
        {label}
      </div>
      {hasHtml ? (
        <a
          className="text-[11px] font-medium text-red-300 underline underline-offset-2"
          href={url}
          target="_blank"
          rel="noreferrer"
        >
          Apri HTML
        </a>
      ) : (
        <span className="text-[11px] text-muted-foreground">HTML non disponibile</span>
      )}
    </div>
  );
}

function ResultsSkeleton() {
  return (
    <div className="overflow-hidden rounded-lg border border-border">
      <div className="bg-muted/40 px-4 py-3 text-sm text-muted-foreground">
        Loading results...
      </div>
      <div className="divide-y divide-border">
        {Array.from({ length: 8 }).map((_, i) => (
          <div key={i} className="grid grid-cols-12 gap-4 px-4 py-4">
            <div className="col-span-4 space-y-2">
              <div className="h-4 w-3/4 animate-pulse rounded bg-muted" />
              <div className="h-3 w-full animate-pulse rounded bg-muted/70" />
            </div>
            <div className="col-span-3 space-y-2">
              <div className="h-3 w-5/6 animate-pulse rounded bg-muted" />
              <div className="h-3 w-3/6 animate-pulse rounded bg-muted/70" />
            </div>
            <div className="col-span-3 space-y-2">
              <div className="h-6 w-4/6 animate-pulse rounded bg-muted" />
              <div className="h-6 w-3/6 animate-pulse rounded bg-muted/70" />
            </div>
            <div className="col-span-2 space-y-2">
              <div className="h-6 w-full animate-pulse rounded bg-muted" />
              <div className="h-6 w-5/6 animate-pulse rounded bg-muted/70" />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function WebsiteStatusBadge({ row }: { row: BusinessResult }) {
  if (row.website_status === "HAS_WEBSITE") {
    return (
      <div className="website-status-cell flex items-center gap-2">
        <Badge variant="success">WEBSITE OK</Badge>
        <Badge variant={row.audit.has_ssl ? "success" : "warning"}>
          {row.audit.has_ssl ? "SSL" : "NO SSL"}
        </Badge>
        <Badge variant={row.audit.is_mobile_responsive ? "success" : "warning"}>
          {row.audit.is_mobile_responsive ? "MOBILE" : "NOT MOBILE"}
        </Badge>
      </div>
    );
  }

  return <Badge variant="danger">NO WEBSITE</Badge>;
}

function hasWebsite(row: BusinessResult): boolean {
  if (row.website_status !== "HAS_WEBSITE") return false;
  const w = (row.website || "").trim();
  return w.length > 0;
}

function TechStackBadge({ row }: { row: BusinessResult }) {
  if (!hasWebsite(row)) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  const raw = String(row.tech_stack || "Custom HTML").trim() || "Custom HTML";
  const key = raw.toLowerCase();
  const variant:
    | "wordpress"
    | "wix"
    | "shopify"
    | "squarespace"
    | "custom" =
    key.includes("wordpress")
      ? "wordpress"
      : key.includes("wix")
        ? "wix"
        : key.includes("shopify")
          ? "shopify"
          : key.includes("squarespace")
            ? "squarespace"
            : "custom";

  return <Badge variant={variant}>{raw}</Badge>;
}

function parseIsoDate(value?: string | null): Date | null {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function daysUntil(d: Date): number {
  const ms = d.getTime() - Date.now();
  return Math.floor(ms / (1000 * 60 * 60 * 24));
}

function yearsSince(d: Date): number {
  const ms = Date.now() - d.getTime();
  return ms / (1000 * 60 * 60 * 24 * 365.25);
}

function isExpiringSoon(expirationIso?: string | null): boolean {
  const d = parseIsoDate(expirationIso);
  if (!d) return false;
  const days = daysUntil(d);
  return days >= 0 && days < 30;
}

function isLegacyBrand(creationIso?: string | null): boolean {
  const d = parseIsoDate(creationIso);
  if (!d) return false;
  return yearsSince(d) >= 10;
}

function LoadSpeedCell({ row }: { row: BusinessResult }) {
  if (!hasWebsite(row)) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  const v = ((row as any).load_speed ?? row.load_speed_s) as unknown;
  const n = typeof v === "string" ? Number.parseFloat(v) : (v as number | null | undefined);
  if (n === undefined || n === null || !Number.isFinite(n)) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  const s = Number(n);
  const text = `${s.toFixed(s < 1 ? 2 : 1)}s`;
  const cls = s < 1.5 ? "text-emerald-300" : s <= 2.5 ? "text-amber-200" : "text-red-300";
  return <span className={cls}>{text}</span>;
}

function ResultsTable({
  rows,
  city,
  jobId,
  onTechnical,
}: {
  rows: BusinessResult[];
  city: string;
  jobId: string;
  onTechnical: (row: BusinessResult) => void;
}) {
  return (
    <div className="results-table-container glass-panel w-full overflow-x-auto whitespace-nowrap pb-5">
      <table className="results-table w-full min-w-[1600px] text-sm">
        <thead className="bg-white/[0.02]">
          <tr className="text-left text-[11px] uppercase tracking-widest text-slate-400">
            <th className="px-5 py-4 font-medium">
              Business Name
            </th>
            <th className="px-5 py-4 font-medium">
              Contacts
            </th>
            <th className="px-5 py-4 font-medium">
              Website Status
            </th>
            <th className="px-5 py-4 font-medium">
              Tech Stack
            </th>
            <th className="px-5 py-4 font-medium">
              Load Speed
            </th>
            <th className="px-5 py-4 font-medium">
              Marketing Audit
            </th>
            <th className="px-5 py-4 font-medium">
              Website Code
            </th>
            <th className="px-2 py-4 font-medium w-[120px]">
              Actions
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => {
            const isSiteDown =
              typeof row.website_http_status === "number" && row.website_http_status >= 400;
            const isCritical =
              row.website_status === "MISSING_WEBSITE" ||
              isSiteDown ||
              Boolean(String(row.website_error ?? "").trim()) ||
              (row.website_status === "HAS_WEBSITE" && !row.audit.has_facebook_pixel);
            const rowClass = isCritical
              ? "row-hover-glow border-t border-white/5 bg-[rgba(239,68,68,0.10)]"
              : "row-hover-glow border-t border-white/5";

            return (
              <motion.tr
                key={`${row.business_name}-${idx}`}
                className={rowClass}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.35, delay: Math.min(idx * 0.03, 0.6) }}
              >
                <td className="px-5 py-5">
                  <div>
                    <div className="font-semibold">{row.business_name}</div>
                    <div className="mt-1 text-xs text-muted-foreground">
                      {row.address ?? ""}
                    </div>
                  </div>
                </td>
                <td className="px-5 py-5">
                  <div className="space-y-2">
                    <div className="flex items-center gap-3">
                      {row.phone ? (
                        <>
                          <a
                            className="inline-flex items-center gap-2 text-muted-foreground hover:text-foreground"
                            href={`tel:${row.phone}`}
                          >
                            <Phone className="h-4 w-4" />
                            <span className="text-xs">{row.phone}</span>
                          </a>

                          {isMobilePhone(row.phone) ? (
                            <span className="rounded-md border border-emerald-500/40 bg-emerald-500/10 px-2 py-1 text-[10px] font-semibold tracking-wide text-emerald-300">
                              MOBILE
                            </span>
                          ) : null}

                          {isWhatsappEligible(row.phone) ? (
                            <a
                              className="inline-flex items-center text-muted-foreground hover:text-foreground"
                              href={`https://wa.me/${cleanPhoneForWhatsApp(row.phone)}`}
                              target="_blank"
                              rel="noreferrer"
                              title="WhatsApp"
                            >
                              <MessageCircle className="h-4 w-4" />
                            </a>
                          ) : null}

                          <button
                            type="button"
                            className="inline-flex items-center text-muted-foreground hover:text-foreground"
                            onClick={() =>
                              window.open(
                                buildSocialSearchUrl(row.business_name, city),
                                "_blank",
                                "noopener,noreferrer",
                              )
                            }
                            title="Find Social"
                          >
                            <Search className="h-4 w-4" />
                          </button>
                        </>
                      ) : (
                        <span className="text-xs text-muted-foreground">No phone</span>
                      )}

                      {row.website ? (
                        <a
                          className="inline-flex items-center gap-2 text-muted-foreground hover:text-foreground"
                          href={row.website}
                          target="_blank"
                          rel="noreferrer"
                        >
                          <ExternalLink className="h-4 w-4" />
                          <span className="text-xs">Website</span>
                        </a>
                      ) : null}
                    </div>

                    {row.email ? (
                      <div className="text-xs text-muted-foreground">{row.email}</div>
                    ) : null}
                  </div>
                </td>
                <td className="px-5 py-5">
                  <WebsiteStatusBadge row={row} />
                </td>
                <td className="px-5 py-5">
                  <TechStackBadge row={row} />
                </td>
                <td className="px-5 py-5">
                  <LoadSpeedCell row={row} />
                </td>
                <td className="px-5 py-5">
                  <MarketingBadges row={row} />
                </td>
                <td className="px-5 py-5">
                  <WebsiteCodeErrorCell row={row} jobId={jobId} />
                </td>
                <td className="px-2 py-5">
                  <div className="flex items-center gap-2 justify-end">
                    <Button
                      size="sm"
                      variant="secondary"
                      onClick={() => onTechnical(row)}
                      disabled={row.website_status !== "HAS_WEBSITE" || !row.website}
                      className="actions-analyze-btn"
                    >
                      <span className="mr-2">🔍</span>
                      ANALISI TECNICA
                    </Button>
                  </div>
                </td>
              </motion.tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function Dashboard() {
  const isDemo = false;

  const CATEGORIES: string[] = [
    "Ristoranti",
    "Pizzerie",
    "Bar",
    "Gelaterie",
    "Hotel",
    "B&B",
    "Parrucchieri",
    "Centri Estetici",
    "Dentisti",
    "Medici",
    "Farmacie",
    "Palestre",
    "Fisioterapisti",
    "Psicologi",
    "Avvocati",
    "Commercialisti",
    "Agenzie Immobiliari",
    "Carrozzerie",
    "Autofficine",
    "Elettricisti",
    "Idraulici",
    "Imprese Edili",
    "Serramentisti",
    "Falegnami",
    "Giardinieri",
    "Pulizie",
    "Ristrutturazioni",
    "Climatizzatori",
    "Energie Rinnovabili",
  ];

  const CITIES: string[] = [
    "Agrigento",
    "Alessandria",
    "Ancona",
    "Andria",
    "Aosta",
    "Arezzo",
    "Ascoli Piceno",
    "Asti",
    "Avellino",
    "Bari",
    "Barletta",
    "Belluno",
    "Benevento",
    "Bergamo",
    "Biella",
    "Bologna",
    "Bolzano",
    "Brescia",
    "Brindisi",
    "Cagliari",
    "Caltanissetta",
    "Campobasso",
    "Caserta",
    "Catania",
    "Catanzaro",
    "Chieti",
    "Como",
    "Cosenza",
    "Cremona",
    "Crotone",
    "Cuneo",
    "Enna",
    "Ferrara",
    "Firenze",
    "Foggia",
    "Forlì",
    "Frosinone",
    "Genova",
    "Grosseto",
    "Imperia",
    "Isernia",
    "L'Aquila",
    "La Spezia",
    "Latina",
    "Lecce",
    "Lecco",
    "Livorno",
    "Lodi",
    "Lucca",
    "Macerata",
    "Mantova",
    "Massa",
    "Matera",
    "Messina",
    "Milano",
    "Modena",
    "Monza",
    "Napoli",
    "Novara",
    "Nuoro",
    "Oristano",
    "Padova",
    "Palermo",
    "Parma",
    "Pavia",
    "Perugia",
    "Pesaro",
    "Pescara",
    "Piacenza",
    "Pisa",
    "Pistoia",
    "Pordenone",
    "Potenza",
    "Prato",
    "Ragusa",
    "Ravenna",
    "Reggio Calabria",
    "Reggio Emilia",
    "Rieti",
    "Rimini",
    "Roma",
    "Rovigo",
    "Salerno",
    "Sassari",
    "Savona",
    "Siena",
    "Siracusa",
    "Sondrio",
    "Taranto",
    "Teramo",
    "Terni",
    "Torino",
    "Trapani",
    "Trento",
    "Treviso",
    "Trieste",
    "Udine",
    "Varese",
    "Venezia",
    "Verona",
    "Vibo Valentia",
    "Vicenza",
    "Viterbo",
  ];

  const categoryPresets = React.useMemo(() => {
    return [...CATEGORIES];
  }, []);

  const [categoryMode, setCategoryMode] = React.useState<"preset" | "custom">(
    CATEGORIES.length ? "preset" : "custom",
  );
  const [categoryPreset, setCategoryPreset] = React.useState(
    CATEGORIES[0] ?? "",
  );
  const [categoryCustom, setCategoryCustom] = React.useState("");

  const category =
    categoryMode === "custom" ? categoryCustom : categoryPreset;

  const cityPresets = React.useMemo(() => {
    return [...CITIES];
  }, []);

  const [cityMode, setCityMode] = React.useState<"preset" | "custom">(
    CITIES.length ? "preset" : "custom",
  );
  const [cityPreset, setCityPreset] = React.useState(CITIES[0] ?? "Milano");
  const [cityCustom, setCityCustom] = React.useState("");
  const city = cityMode === "custom" ? cityCustom : cityPreset;
  const isCityInZoneDb = React.useMemo(() => {
    const c = city.trim();
    if (!c) return false;
    return Object.prototype.hasOwnProperty.call(zone_database, c);
  }, [city]);
  const zoneOptions = React.useMemo(() => {
    if (!isCityInZoneDb) return [] as string[];
    return zone_database[city.trim()] ?? [];
  }, [isCityInZoneDb, city]);

  const [zone, setZone] = React.useState("Tutta la città");

  React.useEffect(() => {
    const c = city.trim();
    if (!c) {
      setZone("Tutta la città");
      return;
    }
    if (!Object.prototype.hasOwnProperty.call(zone_database, c)) {
      setZone("Tutta la città");
      return;
    }
    const opts = zone_database[c] ?? [];
    if (!opts.length) {
      setZone("Tutta la città");
      return;
    }
    if (!zone || !opts.includes(zone)) {
      setZone(opts[0] ?? "Tutta la città");
    }
  }, [city]);

  const debouncedCityQuery = useDebouncedValue<string>(cityMode === "custom" ? cityCustom : "", 250);
  const [citySuggestions, setCitySuggestions] = React.useState<string[]>([]);
  const [citySuggestOpen, setCitySuggestOpen] = React.useState(false);

  const cityStaticOptions = React.useMemo(() => {
    return Array.from(new Set(cityPresets.map((s) => String(s).trim()).filter(Boolean)));
  }, [cityPresets]);

  const cityDropdownOptions = React.useMemo(() => {
    const merged = [...cityStaticOptions, ...citySuggestions];
    return Array.from(new Set(merged.map((s) => String(s).trim()).filter(Boolean)));
  }, [cityStaticOptions, citySuggestions]);

  const cityFilteredOptions = React.useMemo(() => {
    const q = String(cityCustom || "").trim().toLowerCase();
    const list = !q
      ? cityDropdownOptions
      : cityDropdownOptions.filter((s) => s.toLowerCase().includes(q));
    return list.slice(0, 30);
  }, [cityDropdownOptions, cityCustom]);

  React.useEffect(() => {
    if (isDemo) {
      setCitySuggestions([]);
      return;
    }
    if (cityMode !== "custom") {
      setCitySuggestions([]);
      return;
    }
    const q = debouncedCityQuery;
    if (!q || q.length < 2) {
      setCitySuggestions([]);
      return;
    }

    const ac = new AbortController();
    (async () => {
      try {
        const url =
          "https://nominatim.openstreetmap.org/search?format=jsonv2&countrycodes=it&addressdetails=1&limit=8&q=" +
          encodeURIComponent(q);
        const res = await fetch(url, {
          signal: ac.signal,
          headers: {
            "Accept": "application/json",
          },
        });
        if (!res.ok) {
          setCitySuggestions([]);
          return;
        }
        const data = (await res.json()) as Array<any>;
        const names = data
          .map((x) => {
            const a = x?.address;
            return (
              a?.city ||
              a?.town ||
              a?.village ||
              a?.hamlet ||
              a?.municipality ||
              x?.name ||
              ""
            );
          })
          .map((s) => String(s).trim())
          .filter(Boolean);
        const uniq = Array.from(new Set(names));
        setCitySuggestions(uniq);
      } catch (e) {
        if ((e as any)?.name === "AbortError") return;
        setCitySuggestions([]);
      }
    })();

    return () => ac.abort();
  }, [debouncedCityQuery, cityMode]);

  React.useEffect(() => {
    if (!CATEGORIES.length) {
      setCategoryMode("custom");
      return;
    }
    if (!CATEGORIES.includes(categoryPreset)) {
      setCategoryPreset(CATEGORIES[0]);
      setCategoryMode("preset");
      setCategoryCustom("");
    }
  }, [categoryPreset]);

  const [job, setJob] = React.useState<JobStatus | null>(null);
  const [progress, setProgress] = React.useState(0);
  const [message, setMessage] = React.useState("—");
  const [state, setState] = React.useState<string | null>(null);
  const [rows, setRows] = React.useState<BusinessResult[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [sortDir, setSortDir] = React.useState<"desc" | "asc">("desc");
  const [showOnlyMobile, setShowOnlyMobile] = React.useState(false);

  const PAGE_SIZE = 25;
  const [page, setPage] = React.useState(0);

  const highPriorityTargets = React.useMemo(() => {
    return rows.filter((r) => {
      const speedCritical = (r.load_speed_s ?? 0) > 3;
      const noPixel = r.website_status === "HAS_WEBSITE" && !r.audit.has_facebook_pixel;
      const noSite = r.website_status === "MISSING_WEBSITE";
      const expSoon = isExpiringSoon(r.domain_expiration_date ?? null);
      return speedCritical || noPixel || noSite || expSoon;
    }).length;
  }, [rows]);

  const lastResultsCountRef = React.useRef(0);
  const resultsFetchInFlightRef = React.useRef(false);


  const filteredRows = React.useMemo(() => {
    if (!showOnlyMobile) return rows;
    return rows.filter((r) => isMobilePhone(r.phone ?? null));
  }, [rows, showOnlyMobile]);

  const sortedRows = React.useMemo(() => {
    const base = [...filteredRows];
    const score = (r: BusinessResult) => {
      if (r.website_status === "MISSING_WEBSITE") return 2000;
      if (r.website_status === "HAS_WEBSITE" && !r.audit.has_facebook_pixel) return 1500;
      let s = 0;
      if (!r.audit.has_facebook_pixel) s += 200;
      if (!r.audit.has_gtm) s += 60;
      if (!r.audit.has_ssl) s += 30;
      if (!r.audit.is_mobile_responsive) s += 20;
      return s;
    };

    const dir = sortDir === "desc" ? -1 : 1;
    return base.sort((a, b) => (score(a) - score(b)) * dir);
  }, [filteredRows, sortDir]);

  React.useEffect(() => {
    setPage(0);
  }, [sortDir, rows.length, showOnlyMobile]);
  const totalPages = Math.max(1, Math.ceil(sortedRows.length / PAGE_SIZE));
  const pageSafe = Math.min(Math.max(0, page), totalPages - 1);
  const pagedRows = React.useMemo(() => {
    const start = pageSafe * PAGE_SIZE;
    return sortedRows.slice(start, start + PAGE_SIZE);
  }, [sortedRows, pageSafe]);

  async function onStart() {
    setLoading(true);
    setRows([]);
    setProgress(0);
    setMessage("Starting...");
    lastResultsCountRef.current = 0;

    try {
      const zoneValue = String(zone || "").trim();
      const j = await startJob(category, city, zoneValue);
      setJob(j);
      setState(j.state);

      let pollingTimer: number | null = null;
      const stopPolling = () => {
        if (pollingTimer) {
          window.clearInterval(pollingTimer);
          pollingTimer = null;
        }
      };

      const startPolling = () => {
        if (pollingTimer) return;
        pollingTimer = window.setInterval(async () => {
          try {
            const s = await fetchJob(j.id);
            setProgress(s.progress ?? 0);
            setMessage(s.message ?? "");
            setState(s.state ?? null);

            const nextCount = Math.max(0, Number(s.results_count ?? 0) || 0);
            if (
              nextCount > lastResultsCountRef.current &&
              !resultsFetchInFlightRef.current &&
              s.state !== "done"
            ) {
              resultsFetchInFlightRef.current = true;
              lastResultsCountRef.current = nextCount;
              try {
                const data = await fetchResults(j.id);
                setRows(data);
              } finally {
                resultsFetchInFlightRef.current = false;
              }
            }

            if (s.state === "done") {
              stopPolling();
              const data = await fetchResults(j.id);
              setRows(data);
              setLoading(false);
            }

            if (s.state === "error") {
              stopPolling();
              setLoading(false);
              setRows([]);
              setMessage(s.error ? `Errore: ${s.error}` : "Errore durante l'audit");
            }
          } catch {
            // keep trying
          }
        }, 1000);
      };

      const ev = new EventSource(`${BACKEND_URL}/jobs/${j.id}/events`);
      ev.onmessage = async (e) => {
        let parsed: any = null;
        try {
          parsed = JSON.parse(String(e.data || "{}"));
        } catch {
          parsed = null;
        }

        if (!parsed) return;

        if (parsed.progress !== undefined && parsed.progress !== null) {
          setProgress(parsed.progress ?? 0);
        }
        if (parsed.message !== undefined && parsed.message !== null) {
          setMessage(parsed.message ?? "");
        }
        if (parsed.state !== undefined && parsed.state !== null) {
          setState(parsed.state ?? null);
        }

        const nextCount = Math.max(0, Number(parsed.results_count ?? 0) || 0);
        if (
          nextCount > lastResultsCountRef.current &&
          !resultsFetchInFlightRef.current &&
          parsed.state !== "done"
        ) {
          resultsFetchInFlightRef.current = true;
          lastResultsCountRef.current = nextCount;
          try {
            const data = await fetchResults(j.id);
            setRows(data);
          } catch {
            // ignore
          } finally {
            resultsFetchInFlightRef.current = false;
          }
        }

        if (parsed.state === "done") {
          ev.close();
          stopPolling();
          const data = await fetchResults(j.id);
          setRows(data);
          setLoading(false);
        }

        if (parsed.state === "error") {
          ev.close();
          stopPolling();
          setRows([]);
          setLoading(false);
          setMessage(parsed.error ? `Errore: ${parsed.error}` : "Errore durante l'audit");
        }
      };

      ev.onerror = async () => {
        ev.close();
        // SSE can be flaky on some setups; fall back to polling.
        setMessage("SSE connection dropped. Switching to polling...");
        startPolling();
      };
    } catch {
      setMessage("Failed to start job. Ensure backend is running.");
      setLoading(false);
    }
  }

  function exportCsv() {
    if (!job) return;
    window.open(`${BACKEND_URL}/jobs/${job.id}/export.csv`, "_blank");
  }

  const [techOpen, setTechOpen] = React.useState(false);
  const [techRow, setTechRow] = React.useState<BusinessResult | null>(null);
  const [techLoading, setTechLoading] = React.useState(false);
  const [techResult, setTechResult] = React.useState<TechnicalAuditResult | null>(null);
  const [techError, setTechError] = React.useState<string | null>(null);

  async function openTechnicalAudit(row: BusinessResult) {
    if (!job) return;
    setTechRow(row);
    setTechOpen(true);
    setTechLoading(true);
    setTechResult(null);
    setTechError(null);
    try {
      const r = await fetchTechnicalAudit(job.id, row.result_index);
      setTechResult(r);
    } catch (e) {
      setTechError(e instanceof Error ? e.message : "Errore durante l'analisi tecnica");
    } finally {
      setTechLoading(false);
    }
  }

  return (
    <div className="mx-auto px-6 py-20 md:px-10">
      <div className="mx-auto w-[95vw] max-w-[1800px]">
        <div className="text-center">
          <div className="text-xs font-medium tracking-widest text-slate-400">
            PREMIUM AUDIT
          </div>
          <h1 className="mt-6 text-[3.75rem] font-extrabold leading-[1.02] tracking-tight text-white">
            Lead Generation Machine
          </h1>
          <p className="mx-auto mt-4 max-w-2xl text-sm text-slate-400">
            Inserisci categoria e città. Aspetta ~2 minuti. Ottieni un audit con opportunità in rosso
            (sito mancante / pixel mancanti).
          </p>

          <div className="mt-12">
            <div className="glass-panel px-10 py-8">
              <div className="text-xs font-medium tracking-widest text-slate-400">
                HIGH PRIORITY TARGETS
              </div>
              <div className="mt-3 flex items-end justify-center gap-3">
                <div className="text-5xl font-semibold tracking-tight text-slate-100">
                  {highPriorityTargets}
                </div>
                <div className="pb-1 text-sm font-medium text-slate-400">targets</div>
              </div>
              <div className="mt-3 text-sm text-slate-400">
                Speed &gt; 3s, NO PIXEL, NO SITE, or domain expiring soon.
              </div>
              <div className="mt-5 flex justify-center">
                <Button
                  variant="secondary"
                  onClick={exportCsv}
                  disabled={!job || rows.length === 0}
                >
                  <FileDown className="mr-2 h-4 w-4" />
                  Export to CSV
                </Button>
              </div>
            </div>
          </div>
        </div>

        <div className="mt-16">
          <div className="glass-capsule relative z-50 flex flex-col gap-5 p-5 md:flex-row md:items-center">
            <div className="space-y-2">
              <div className="text-xs text-muted-foreground">Categoria Target</div>
              <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
                <select
                  value={categoryMode === "custom" ? "__custom__" : categoryPreset}
                  onChange={(e) => {
                    const v = e.target.value;
                    if (v === "__custom__") {
                      setCategoryMode("custom");
                      return;
                    }
                    setCategoryMode("preset");
                    setCategoryPreset(v);
                  }}
                  className="h-[60px] w-full rounded-xl border border-white/[0.08] bg-transparent px-5 text-base text-slate-100 shadow-sm outline-none transition-colors focus-visible:border-violet-400/40 focus-visible:ring-0 focus-visible:shadow-[0_0_20px_rgba(139,92,246,0.25)]"
                >
                  {categoryPresets.map((p) => (
                    <option key={p} value={p}>
                      {p}
                    </option>
                  ))}
                  <option value="__custom__">Custom…</option>
                </select>

                <Input
                  value={categoryCustom}
                  onChange={(e) => {
                    setCategoryMode("custom");
                    setCategoryCustom(e.target.value);
                  }}
                  placeholder="Custom category..."
                  disabled={false}
                  className=""
                />
              </div>
            </div>
            <div className="space-y-2">
              <div className="text-xs text-muted-foreground">City</div>
              <div className="space-y-3">
                <div>
                  <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
                    <select
                      value={cityMode === "custom" ? "__custom__" : cityPreset}
                      onChange={(e) => {
                        const v = e.target.value;
                        if (v === "__custom__") {
                          setCityMode("custom");
                          return;
                        }
                        setCityMode("preset");
                        setCityPreset(v);
                      }}
                      className="h-[60px] w-full rounded-xl border border-white/[0.08] bg-transparent px-5 text-base text-slate-100 shadow-sm outline-none transition-colors focus-visible:border-violet-400/40 focus-visible:ring-0 focus-visible:shadow-[0_0_20px_rgba(139,92,246,0.25)]"
                    >
                      {cityPresets.map((p) => (
                        <option key={p} value={p}>
                          {p}
                        </option>
                      ))}
                      <option value="__custom__">Cerca…</option>
                    </select>

                    <Input
                      list="city-options"
                      placeholder="Scrivi città/paese…"
                      value={cityCustom}
                      disabled={cityMode !== "custom"}
                      onChange={(e) => setCityCustom(e.target.value)}
                    />
                  </div>
                  <datalist id="city-options">
                    {cityFilteredOptions.map((s) => (
                      <option key={s} value={s} />
                    ))}
                  </datalist>
                </div>

                <div>
                  <div className="mb-2 text-xs text-muted-foreground">Zona</div>
                  <Input
                    list={isCityInZoneDb ? "zone-options" : undefined}
                    placeholder={
                      isCityInZoneDb
                        ? "Seleziona o scrivi zona…"
                        : "Scrivi zona (opzionale)…"
                    }
                    value={zone}
                    onChange={(e) => setZone(e.target.value)}
                  />
                  {isCityInZoneDb ? (
                    <datalist id="zone-options">
                      {zoneOptions.map((z) => (
                        <option key={z} value={z} />
                      ))}
                    </datalist>
                  ) : null}
                </div>
              </div>
            </div>

            <div className="flex items-end justify-center md:justify-end">
              <Button
                className="btn-glow neon-glow h-[60px] w-full px-8 text-white hover:opacity-95 md:w-auto"
                onClick={onStart}
                disabled={loading || category.trim().length < 2 || city.trim().length < 2}
              >
                <Rocket className="mr-2 h-4 w-4" />
                Start Audit
              </Button>
            </div>
          </div>

          <div className="mt-4">
            <Progress value={progress} />
            <div className="mt-2 flex items-center justify-between text-xs text-slate-400">
              <span>{message || "—"}</span>
              <span>{progress}%</span>
            </div>
          </div>

          <AnimatePresence>

          {!loading && rows.length === 0 ? (
            <motion.div
              key="empty"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: 10 }}
              className="glass-panel p-8"
            >
              <div className="text-sm font-medium">No results yet</div>
              <div className="mt-2 text-sm text-muted-foreground">
                Inserisci categoria e città e premi <span className="text-foreground">Start Deep Audit</span>.
              </div>
            </motion.div>
          ) : null}

          {rows.length > 0 ? (
            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: 10 }}
              className="space-y-3"
            >
              {sortedRows.length > PAGE_SIZE ? (
                <div className="flex items-center justify-center gap-3 text-xs text-slate-400">
                  <Button
                    size="sm"
                    variant="secondary"
                    onClick={() => setPage((p) => Math.max(0, p - 1))}
                    disabled={pageSafe === 0}
                  >
                    Prev
                  </Button>
                  <span>
                    Page {pageSafe + 1} / {totalPages}
                  </span>
                  <Button
                    size="sm"
                    variant="secondary"
                    onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
                    disabled={pageSafe >= totalPages - 1}
                  >
                    Next
                  </Button>
                </div>
              ) : null}

              <div className="flex items-center justify-center">
                <Button
                  variant="secondary"
                  onClick={() => setSortDir((d) => (d === "desc" ? "asc" : "desc"))}
                >
                  {sortDir === "desc" ? (
                    <ArrowDown className="mr-2 h-4 w-4" />
                  ) : (
                    <ArrowUp className="mr-2 h-4 w-4" />
                  )}
                  Sort by priority
                </Button>

                <Button
                  variant={showOnlyMobile ? "default" : "secondary"}
                  className="ml-3"
                  onClick={() => setShowOnlyMobile((v) => !v)}
                  disabled={rows.length === 0}
                >
                  📱 MOSTRA SOLO CELLULARI
                </Button>
              </div>

              <ResultsTable
                rows={pagedRows}
                city={city}
                jobId={job!.id}
                onTechnical={(row) => {
                  openTechnicalAudit(row);
                }}
              />
            </motion.div>
          ) : null}

        </AnimatePresence>
      </div>

      <Dialog
        open={techOpen}
        onOpenChange={(v) => {
          setTechOpen(v);
          if (!v) {
            setTechRow(null);
            setTechResult(null);
            setTechError(null);
          }
        }}
      >
        <DialogContent className="sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>🔍 ANALISI TECNICA</DialogTitle>
            <DialogDescription>
              {techRow?.business_name ?? ""}
            </DialogDescription>
          </DialogHeader>

          {techRow ? (
            <div className="flex flex-wrap items-center gap-2">
              {isExpiringSoon(techRow.domain_expiration_date ?? null) ? (
                <Badge variant="expiring_soon">EXPIRING SOON</Badge>
              ) : null}
              {isLegacyBrand(techRow.domain_creation_date ?? null) ? (
                <Badge variant="legacy_brand">LEGACY BRAND</Badge>
              ) : null}
              {techRow.domain_creation_date ? (
                <span className="text-xs text-slate-400">
                  Created: {techRow.domain_creation_date}
                </span>
              ) : null}
              {techRow.domain_expiration_date ? (
                <span className="text-xs text-slate-400">
                  Expires: {techRow.domain_expiration_date}
                </span>
              ) : null}
              {techRow.load_speed_s !== null && techRow.load_speed_s !== undefined ? (
                <span className="text-xs text-slate-400">
                  Load: {techRow.load_speed_s.toFixed(2)}s
                </span>
              ) : null}
            </div>
          ) : null}

          <div className="rounded-md border border-border bg-[#0b0b0c] p-4 font-mono text-[12px] leading-relaxed text-[#eaeaea]">
            {techLoading ? (
              <div>Analisi in corso...</div>
            ) : techError ? (
              <div className="text-red-300">{techError}</div>
            ) : techResult ? (
              <div className="space-y-3">
                {techResult.issues.length === 0 ? (
                  <div>Nessun errore critico rilevato.</div>
                ) : (
                  techResult.issues.map((it, i) => (
                    <div key={`${it.code}-${i}`} className="rounded border border-red-500/30 bg-red-500/10 p-3">
                      <div className="text-red-200 font-semibold">
                        {it.severity.toUpperCase()}: {it.message}
                      </div>
                      {it.line ? (
                        <div className="mt-1 text-red-100/80">Linea: {it.line}</div>
                      ) : null}
                      {it.context ? (
                        <pre className="mt-2 whitespace-pre-wrap text-[#cbd5e1]">{it.context}</pre>
                      ) : null}
                    </div>
                  ))
                )}
              </div>
            ) : (
              <div>—</div>
            )}
          </div>

          <DialogFooter>
            <Button variant="secondary" onClick={() => setTechOpen(false)}>
              Chiudi
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      </div>
    </div>
  );
}
