import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api } from "./api";

interface ExclusionsPayload {
  exclusions: Record<string, Record<string, { reason: string; at: number }>>;
}

/** Scrips the holder has judged unsellable — suspended, delisted, written off.
 *
 *  Kept out of deployed capital and the return, because a ratio measured against
 *  money that cannot come back is wrong by that much. This is a judgement, not a
 *  fact about the position, so it is stored separately and is always reversible.
 */
export function useExclusions() {
  const queryClient = useQueryClient();
  const { data } = useQuery({
    queryKey: ["exclusions"],
    queryFn: () => api.get<ExclusionsPayload>("/exclusions"),
    staleTime: 30_000,
  });

  const invalidate = () => {
    // Every figure downstream of deployed capital moves with this.
    for (const key of ["exclusions", "portfolio", "positions"]) {
      queryClient.invalidateQueries({ queryKey: [key] });
    }
  };

  const exclude = useMutation({
    mutationFn: (v: { account: string; symbol: string; reason: string }) =>
      api.post("/exclusions", v),
    onSuccess: invalidate,
  });
  const restore = useMutation({
    mutationFn: (v: { account: string; symbol: string }) =>
      api.del(`/exclusions?account=${encodeURIComponent(v.account)}` +
              `&symbol=${encodeURIComponent(v.symbol)}`),
    onSuccess: invalidate,
  });

  const map = data?.exclusions ?? {};
  return {
    isExcluded: (account: string, symbol: string) => Boolean(map[account]?.[symbol]),
    reasonFor: (account: string, symbol: string) => map[account]?.[symbol]?.reason ?? "",
    count: Object.values(map).reduce((n, byAccount) => n + Object.keys(byAccount).length, 0),
    exclude: exclude.mutate,
    restore: restore.mutate,
    busy: exclude.isPending || restore.isPending,
  };
}
