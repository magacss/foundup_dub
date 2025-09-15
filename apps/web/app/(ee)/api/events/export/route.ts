import { getEvents } from "@/lib/analytics/get-events";
import { getFolderIdsToFilter } from "@/lib/analytics/get-folder-ids-to-filter";
import { convertToCSV, validDateRangeForPlan } from "@/lib/analytics/utils";
import { getDomainOrThrow } from "@/lib/api/domains/get-domain-or-throw";
import { getLinkOrThrow } from "@/lib/api/links/get-link-or-throw";
import { throwIfClicksUsageExceeded } from "@/lib/api/links/usage-checks";
import { withWorkspace } from "@/lib/auth";
import { verifyFolderAccess } from "@/lib/folder/permissions";
import { ClickEvent, LeadEvent, SaleEvent } from "@/lib/types";
import { eventsQuerySchema } from "@/lib/zod/schemas/analytics";
import { COUNTRIES, capitalize } from "@dub/utils";
import { z } from "zod";

// WICHTIG für Prisma/Node-APIs: in der Node-Runtime laufen.
export const runtime = "nodejs";

type Row = ClickEvent | LeadEvent | SaleEvent;

const columnNames: Record<string, string> = {
  trigger: "Event",
  url: "Destination URL",
  os: "OS",
  referer: "Referrer",
  refererUrl: "Referrer URL",
  timestamp: "Date",
  invoiceId: "Invoice ID",
  saleAmount: "Sale Amount",
  clickId: "Click ID",
};

const columnAccessors: Record<
  string,
  (r: Row | LeadEvent | SaleEvent | ClickEvent) => any
> = {
  trigger: (r: Row) => (r as ClickEvent).click.trigger,
  event: (r: LeadEvent | SaleEvent) => (r as LeadEvent | SaleEvent).eventName,
  url: (r: ClickEvent) => r.click.url,
  link: (r: Row) => (r as any).domain + ((r as any).key === "_root" ? "" : `/${(r as any).key}`),
  country: (r: Row) =>
    (r as any).country ? COUNTRIES[(r as any).country] ?? (r as any).country : (r as any).country,
  referer: (r: ClickEvent) => r.click.referer,
  refererUrl: (r: ClickEvent) => r.click.refererUrl,
  customer: (r: LeadEvent | SaleEvent) =>
    (r as any).customer.name +
    ((r as any).customer.email ? ` <${(r as any).customer.email}>` : ""),
  invoiceId: (r: SaleEvent) => (r as any).sale.invoiceId,
  saleAmount: (r: SaleEvent) => "$" + ((r as any).sale.amount / 100).toFixed(2),
  clickId: (r: ClickEvent) => r.click.id,
};

// Lokales Schema: erlaubt die zusätzlichen (optionalen) Felder + columns als CSV
const LocalQuerySchema = eventsQuerySchema
  .extend({
    domain: z.string().optional(),
    key: z.string().optional(),
    interval: z.string().optional(),
    start: z.string().optional(),
    end: z.string().optional(),
    folderId: z.string().optional(),
  })
  .and(
    z.object({
      columns: z
        .string()
        .transform((c) => c.split(",").map((s) => s.trim()).filter(Boolean))
        .pipe(z.string().array()),
    })
  );

// GET /api/events/export – get export data for analytics
export const GET = withWorkspace(async ({ searchParams, workspace, session }) => {
  // Nutzungs-Limits prüfen
  throwIfClicksUsageExceeded(workspace);

  // URLSearchParams -> Plain Object (wichtig für Zod)
  const queryObject =
    typeof searchParams?.get === "function"
      ? Object.fromEntries(searchParams as URLSearchParams)
      : (searchParams as any);

  const parsedParams = LocalQuerySchema.parse(queryObject);

  const { event, domain, interval, start, end, columns, key, folderId } =
    parsedParams;

  if (domain) {
    await getDomainOrThrow({ workspace, domain });
  }

  const link =
    domain && key
      ? await getLinkOrThrow({ workspaceId: workspace.id, domain, key })
      : null;

  const folderIdToVerify = link?.folderId || folderId;
  if (folderIdToVerify) {
    await verifyFolderAccess({
      workspace,
      userId: session.user.id,
      folderId: folderIdToVerify,
      requiredPermission: "folders.read",
    });
  }

  validDateRangeForPlan({
    plan: workspace.plan,
    dataAvailableFrom: workspace.createdAt,
    interval,
    start,
    end,
    throwError: true,
  });

  const folderIds =
    folderIdToVerify
      ? undefined
      : await getFolderIdsToFilter({
          workspace,
          userId: session.user.id,
        });

  const response = await getEvents({
    ...parsedParams,
    ...(link && { linkId: link.id }),
    workspaceId: workspace.id,
    limit: 100000,
    folderIds,
    folderId: folderId || "",
  });

  const data = response.map((row) =>
    Object.fromEntries(
      columns.map((c) => [
        columnNames?.[c] ?? capitalize(c),
        columnAccessors[c]?.(row as any) ?? (row as any)?.[c],
      ])
    )
  );

  const csvData = convertToCSV(data);

  return new Response(csvData, {
    headers: {
      "Content-Type": "application/csv",
      "Content-Disposition": `attachment; filename=${event}_export.csv`,
    },
  });
}, {
  requiredPlan: [
    "business",
    "business plus",
    "business extra",
    "business max",
    "advanced",
    "enterprise",
  ],
  requiredPermissions: ["analytics.read"],
});
