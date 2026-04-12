#!/bin/bash
POOL="rbd_pool"
NS="rke-tst"
BLOCKPOOL="${POOL}/${NS}"
INPUT_FILE="discovery_data.json"

if [[ ! -f "${INPUT_FILE}" ]]; then
    echo "Error: ${INPUT_FILE} not found. Run 01_discovery.sh first."
    exit 1
fi

TOTAL=$(jq length ${INPUT_FILE})
if [[ "${TOTAL}" -eq 0 ]]; then
    echo "No chains found in ${INPUT_FILE}. Nothing to clean up."
    exit 0
fi

echo "Found ${TOTAL} chain(s) to process."
echo ""

for i in $(seq 0 $((TOTAL - 1))); do
    PARENT_ID=$(jq -r ".[$i].parent_id" ${INPUT_FILE})
    PARENT_NAME=$(jq -r ".[$i].parent_name" ${INPUT_FILE})
    CSI_SNAP_COUNT=$(jq ".[$i].csi_snaps | length" ${INPUT_FILE})

    echo "=============================================="
    echo "Cleanup: ${PARENT_NAME} (ID: ${PARENT_ID}) [TRASH]"
    echo "  csi-snap children: ${CSI_SNAP_COUNT}"
    echo "=============================================="
    echo ""

    # Process each csi-snap child (bottom-up per csi-snap)
    for s in $(seq 0 $((CSI_SNAP_COUNT - 1))); do
        CS_NAME=$(jq -r ".[$i].csi_snaps[$s].name" ${INPUT_FILE})
        SNAP_NAME=$(jq -r ".[$i].csi_snaps[$s].snapshot.name // empty" ${INPUT_FILE})
        SNAP_ID=$(jq -r ".[$i].csi_snaps[$s].snapshot.id // empty" ${INPUT_FILE})
        CLONE_COUNT=$(jq ".[$i].csi_snaps[$s].clones | length" ${INPUT_FILE})

        echo "  --- ${CS_NAME} ---"

        # Step 7: Handle clone children first (innermost)
        for c in $(seq 0 $((CLONE_COUNT - 1))); do
            C_NAME=$(jq -r ".[$i].csi_snaps[$s].clones[$c].name" ${INPUT_FILE})
            C_PV=$(jq -r ".[$i].csi_snaps[$s].clones[$c].has_pv" ${INPUT_FILE})
            CLONE="${BLOCKPOOL}/${C_NAME}"

            if [[ "${C_PV}" == "true" ]]; then
                echo "  Clone ${CLONE} has PV. Flattening only (keeping alive)..."
                echo "  [CMD] rbd flatten ${CLONE}"
                rbd flatten ${CLONE}
            else
                read -rp "  Delete clone ${CLONE}? [y/n]: " ans < /dev/tty
                if [[ "${ans,,}" == "y" ]]; then
                    echo "  [CMD] rbd rm ${CLONE}"
                    rbd rm ${CLONE}
                    echo "  Deleted ${CLONE}"
                else
                    echo "  Skipped deletion of ${CLONE}"
                fi
            fi
        done

        # Step 8: Delete snapshot on csi-snap
        if [[ -n "${SNAP_NAME}" ]]; then
            SNAP="${BLOCKPOOL}/${CS_NAME}@${SNAP_NAME}"
            read -rp "  Delete snapshot ${SNAP}? [y/n]: " ans < /dev/tty
            if [[ "${ans,,}" == "y" ]]; then
                echo "  [CMD] rbd snap rm ${SNAP}"
                rbd snap rm ${SNAP}
                echo "  Deleted snapshot ${SNAP}"
            else
                echo "  Skipped deletion of ${SNAP}"
            fi
        fi

        # Step 9: Delete csi-snap image
        read -rp "  Delete image ${BLOCKPOOL}/${CS_NAME}? [y/n]: " ans < /dev/tty
        if [[ "${ans,,}" == "y" ]]; then
            echo "  [CMD] rbd rm ${BLOCKPOOL}/${CS_NAME}"
            rbd rm ${BLOCKPOOL}/${CS_NAME}
            echo "  Deleted ${CS_NAME}"
        else
            echo "  Skipped deletion of ${CS_NAME}"
        fi

        echo ""
    done

    # Step 10: Restore csi-vol from trash
    echo "Restoring ${PARENT_NAME} (ID: ${PARENT_ID}) from trash..."
    echo "[CMD] rbd trash restore -p ${POOL} --namespace ${NS} --image-id ${PARENT_ID}"
    rbd trash restore -p ${POOL} --namespace ${NS} --image-id ${PARENT_ID}

    # Step 11: Delete csi-vol
    read -rp "Delete image ${BLOCKPOOL}/${PARENT_NAME}? [y/n]: " ans < /dev/tty
    if [[ "${ans,,}" == "y" ]]; then
        echo "[CMD] rbd rm ${BLOCKPOOL}/${PARENT_NAME}"
        rbd rm ${BLOCKPOOL}/${PARENT_NAME}
        echo "Deleted ${PARENT_NAME}"
    else
        echo "Skipped deletion of ${PARENT_NAME}"
    fi

    echo ""
done

echo "Cleanup complete."
