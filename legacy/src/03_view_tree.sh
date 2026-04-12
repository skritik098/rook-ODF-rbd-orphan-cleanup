#!/bin/bash
# Pretty-print the discovery data as a dependency tree
INPUT_FILE="discovery_data.json"

if [[ ! -f "${INPUT_FILE}" ]]; then
    echo "Error: ${INPUT_FILE} not found. Run 01_discovery.sh first."
    exit 1
fi

echo "===== RBD Dependency Tree ====="
echo ""

jq -r '.[] |
  "\(.parent_name) (ID: \(.parent_id)) [TRASH]",
  ( .csi_snaps[] |
    "  └── \(.name)",
    ( if .snapshot then
        "        └── snap: \(.snapshot.name) (ID: \(.snapshot.id))"
      else
        "        └── snap: (none)"
      end
    ),
    ( .clones[] |
      if .has_pv then
        "              └── clone: \(.name) [HAS PV - will flatten]"
      else
        "              └── clone: \(.name) [NO PV - will delete]"
      end
    )
  ),
  "---"
' ${INPUT_FILE}

echo ""
echo "Summary:"
echo "  Total parent images:          $(jq length ${INPUT_FILE})"
echo "  Total csi-snap children:      $(jq '[.[].csi_snaps | length] | add // 0' ${INPUT_FILE})"
echo "  Total clones (has PV):        $(jq '[.[].csi_snaps[].clones[] | select(.has_pv == true)] | length' ${INPUT_FILE})"
echo "  Total clones (no PV):         $(jq '[.[].csi_snaps[].clones[] | select(.has_pv == false)] | length' ${INPUT_FILE})"
