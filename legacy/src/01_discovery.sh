#!/bin/bash
POOL="ocs-storagecluster-cephblockpool"
OUTPUT_FILE="discovery_data.json"
RBD_CMD="oc -n openshift-storage rsh \$(oc get pods -n openshift-storage -l app=rook-ceph-tools -o name) rbd"

# Start with empty JSON array
echo "[]" > ${OUTPUT_FILE}

# Cache k8s data once
echo "=============================================="
echo "Fetching k8s VSC and PV data..."
echo "=============================================="
echo ""
echo "[DEBUG] Executing: oc get vsc -oyaml"
VSC_DATA=$(oc get vsc -oyaml 2>/dev/null)
echo "[DEBUG] VSC data fetched successfully"
echo ""
echo "[DEBUG] Executing: oc get pv -oyaml"
PV_DATA=$(oc get pv -oyaml 2>/dev/null)
echo "[DEBUG] PV data fetched successfully"
echo ""

for IMAGE_ID in $(eval "${RBD_CMD} trash ls ${POOL}" | grep "csi-vol" | awk '{print $1}'); do
    echo ""
    echo "=============================================="
    echo "PROCESSING PARENT IMAGE"
    echo "=============================================="
    
    # Get parent image name
    echo "[DEBUG] Executing: ${RBD_CMD} info -p ${POOL} --image-id ${IMAGE_ID}"
    IMAGE_NAME=$(eval "${RBD_CMD} info -p ${POOL} --image-id ${IMAGE_ID}" 2>/dev/null | grep "rbd image" | awk -F"'" '{print $2}')
    echo "[INFO] Parent Image: ${IMAGE_NAME}"
    echo "[INFO] Parent ID: ${IMAGE_ID}"
    echo "[INFO] Location: TRASH"
    echo ""

    # Step 2: Find snapshots on trashed csi-vol
    echo "[DEBUG] Executing: ${RBD_CMD} snap ls -p ${POOL} --image-id ${IMAGE_ID} --all"
    PARENT_SNAPS=$(eval "${RBD_CMD} snap ls -p ${POOL} --image-id ${IMAGE_ID} --all" 2>/dev/null | tail -n +2)

    if [[ -z "${PARENT_SNAPS}" ]]; then
        echo "[INFO] No snapshots found on ${IMAGE_NAME}. Skipping."
        echo ""
        continue
    fi
    echo "[INFO] Snapshots found on parent:"
    echo "${PARENT_SNAPS}"
    echo ""
    
    # Debug: Count lines and show each line
    SNAP_LINE_COUNT=$(echo "${PARENT_SNAPS}" | wc -l)
    echo "[DEBUG] Number of snapshot lines to process: ${SNAP_LINE_COUNT}"
    echo "[DEBUG] Iterating through each line:"
    LINE_NUM=0
    while IFS= read -r DEBUG_LINE; do
        LINE_NUM=$((LINE_NUM + 1))
        echo "[DEBUG]   Line ${LINE_NUM}: ${DEBUG_LINE}"
    done <<< "${PARENT_SNAPS}"
    echo ""

    # Build base JSON for this chain
    CHAIN_JSON=$(jq -n \
        --arg pid "${IMAGE_ID}" \
        --arg pname "${IMAGE_NAME}" \
        '{parent_id: $pid, parent_name: $pname, csi_snaps: []}')

    # Find ALL csi-snap child images across all snapshots on the parent
    echo "[DEBUG] Starting snapshot iteration loop..."
    SNAP_COUNT=0
    while IFS= read -r SNAP_LINE; do
        [[ -z "${SNAP_LINE}" ]] && continue
        SNAP_COUNT=$((SNAP_COUNT + 1))
        S_ID=$(echo "${SNAP_LINE}" | awk '{print $1}')
        S_NAME=$(echo "${SNAP_LINE}" | awk '{print $2}')

        echo "----------------------------------------------"
        echo "[INFO] Processing snapshot ${SNAP_COUNT}: ${S_NAME} (ID: ${S_ID})"
        echo ""
        
        echo "[DEBUG] Executing: ${RBD_CMD} children -p ${POOL} --image-id ${IMAGE_ID} --snap-id ${S_ID} --all"
        CHILD_OUT=$(eval "${RBD_CMD} children -p ${POOL} --image-id ${IMAGE_ID} --snap-id ${S_ID} --all" 2>/dev/null)
        
        # Print the output for debugging
        if [[ -n "${CHILD_OUT}" ]]; then
            echo "[DEBUG] Children output:"
            echo "${CHILD_OUT}"
            echo ""
        else
            echo "[DEBUG] No children found in pool for this snapshot"
            echo ""
        fi

        # Search for child-clone images (csi-snap) in both normal pool and trash
        # Process ALL children, not just the first one
        if [[ -n "${CHILD_OUT}" ]] && echo "${CHILD_OUT}" | grep -q "csi-snap"; then
            # Process each child found in the pool
            while IFS= read -r CHILD_LINE; do
                [[ -z "${CHILD_LINE}" ]] && continue
                CSI_SNAP_NAME=$(echo "${CHILD_LINE}" | awk '{print $1}' | sed 's|.*/||')
                echo "[INFO] Found child in pool: ${CSI_SNAP_NAME} (from snapshot: ${S_NAME})"
                echo ""

                # Step 3: Check VSC for this csi-snap using volumeHandle UUID matching
                echo "[DEBUG] Checking VSC for ${CSI_SNAP_NAME}..."
                # Extract UUID from parent image name (last part after last hyphen)
                PARENT_UUID=$(echo "${IMAGE_NAME}" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
                VSC_CHECK=""
                if [[ -n "${PARENT_UUID}" ]]; then
                    echo "[DEBUG] Parent UUID: ${PARENT_UUID}"
                    VSC_CHECK=$(echo "${VSC_DATA}" | grep "volumeHandle:" | grep "${PARENT_UUID}")
                fi
                
                if [[ -n "${VSC_CHECK}" ]]; then
                    echo "[INFO] VSC found for parent ${IMAGE_NAME} (UUID: ${PARENT_UUID}). Skipping csi-snap ${CSI_SNAP_NAME}."
                    echo ""
                    continue
                fi
                echo "[INFO] No VSC found for parent ${IMAGE_NAME}. Including ${CSI_SNAP_NAME} in cleanup."
                echo ""

                # Build csi-snap JSON entry
                CSI_SNAP_JSON=$(jq -n \
                    --arg name "${CSI_SNAP_NAME}" \
                    '{name: $name, snapshot: null, clones: []}')

                # Step 4: Find the snapshot on this csi-snap image (one snapshot per csi-snap)
                echo "[DEBUG] Executing: ${RBD_CMD} snap ls ${POOL}/${CSI_SNAP_NAME} --all"
                CSI_SNAP_SNAP=$(eval "${RBD_CMD} snap ls ${POOL}/${CSI_SNAP_NAME} --all" 2>/dev/null | tail -n +2 | head -1)

                if [[ -n "${CSI_SNAP_SNAP}" ]]; then
                    CS_ID=$(echo "${CSI_SNAP_SNAP}" | awk '{print $1}')
                    CS_NAME=$(echo "${CSI_SNAP_SNAP}" | awk '{print $2}')
                    echo "[INFO] Snapshot on ${CSI_SNAP_NAME}: ${CS_NAME} (ID: ${CS_ID})"
                    echo ""

                    CSI_SNAP_JSON=$(echo "${CSI_SNAP_JSON}" | jq \
                        --arg sname "${CS_NAME}" \
                        --arg sid "${CS_ID}" \
                        '.snapshot = {name: $sname, id: $sid}')

                    # Step 5: Find clone children of this snapshot
                    echo "[DEBUG] Executing: ${RBD_CMD} children ${POOL}/${CSI_SNAP_NAME} --snap-id ${CS_ID} --all"
                    CLONE_OUT=$(eval "${RBD_CMD} children ${POOL}/${CSI_SNAP_NAME} --snap-id ${CS_ID} --all" 2>/dev/null)

                    if [[ -n "${CLONE_OUT}" ]]; then
                        echo "[INFO] Found clones for ${CSI_SNAP_NAME}:"
                        while IFS= read -r CLONE_LINE; do
                            [[ -z "${CLONE_LINE}" ]] && continue
                            CLONE_NAME=$(echo "${CLONE_LINE}" | awk '{print $1}' | sed 's|.*/||')

                            # Step 6: Check PV for clone
                            echo "[DEBUG] Checking PV for clone: ${CLONE_NAME}"
                            PV_CHECK=$(echo "${PV_DATA}" | grep "${CLONE_NAME}")
                            if [[ -n "${PV_CHECK}" ]]; then
                                HAS_PV=true
                                echo "[INFO]   - ${CLONE_NAME} [HAS PV]"
                            else
                                HAS_PV=false
                                echo "[INFO]   - ${CLONE_NAME} [NO PV]"
                            fi

                            CSI_SNAP_JSON=$(echo "${CSI_SNAP_JSON}" | jq \
                                --arg cname "${CLONE_NAME}" \
                                --argjson hpv "${HAS_PV}" \
                                '.clones += [{name: $cname, has_pv: $hpv}]')
                        done <<< "${CLONE_OUT}"
                        echo ""
                    fi
                else
                    echo "[INFO] No snapshot on ${CSI_SNAP_NAME}."
                    echo ""
                fi

                # Add this csi-snap to chain
                CHAIN_JSON=$(echo "${CHAIN_JSON}" | jq --argjson cs "${CSI_SNAP_JSON}" '.csi_snaps += [$cs]')
            done <<< "$(echo "${CHILD_OUT}" | grep "csi-snap")"
        fi
        
        # Also check trash for child-clone images for this snapshot
        # This runs for EVERY snapshot, not just when pool children are empty
        echo "[DEBUG] Checking trash for additional children of snapshot ${S_NAME}..."
        echo "[DEBUG] Executing: ${RBD_CMD} trash ls ${POOL}"
        TRASH_SNAPS=$(eval "${RBD_CMD} trash ls ${POOL}" 2>/dev/null | grep "csi-snap")
        
        if [[ -n "${TRASH_SNAPS}" ]]; then
            echo "[DEBUG] Trash output:"
            echo "${TRASH_SNAPS}"
            echo ""
            
            while IFS= read -r TRASH_LINE; do
                [[ -z "${TRASH_LINE}" ]] && continue
                TRASH_ID=$(echo "${TRASH_LINE}" | awk '{print $1}')
                TRASH_NAME=$(echo "${TRASH_LINE}" | awk '{print $2}')
                
                # Check if this trash image is a child of our parent snapshot
                echo "[DEBUG] Checking trash image: ${TRASH_NAME} (ID: ${TRASH_ID})"
                echo "[DEBUG] Executing: ${RBD_CMD} info -p ${POOL} --image-id ${TRASH_ID}"
                PARENT_CHECK=$(eval "${RBD_CMD} info -p ${POOL} --image-id ${TRASH_ID}" 2>/dev/null | grep "parent:")
                
                if [[ -n "${PARENT_CHECK}" ]]; then
                    echo "[DEBUG] Parent info: ${PARENT_CHECK}"
                fi
                
                if echo "${PARENT_CHECK}" | grep -q "${IMAGE_ID}\|${IMAGE_NAME}"; then
                    CSI_SNAP_NAME="${TRASH_NAME}"
                    echo "[INFO] Found child in trash: ${CSI_SNAP_NAME} (from snapshot: ${S_NAME})"
                    echo ""
                    
                    # Check VSC using UUID matching
                    PARENT_UUID=$(echo "${IMAGE_NAME}" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
                    VSC_CHECK=""
                    if [[ -n "${PARENT_UUID}" ]]; then
                        VSC_CHECK=$(echo "${VSC_DATA}" | grep "volumeHandle:" | grep "${PARENT_UUID}")
                    fi
                    
                    if [[ -n "${VSC_CHECK}" ]]; then
                        echo "[INFO] VSC found for parent ${IMAGE_NAME}. Skipping csi-snap ${CSI_SNAP_NAME}."
                        echo ""
                        continue
                    fi
                    
                    # Process this trash child (same logic as pool children)
                    CSI_SNAP_JSON=$(jq -n --arg name "${CSI_SNAP_NAME}" '{name: $name, snapshot: null, clones: []}')
                    
                    echo "[DEBUG] Executing: ${RBD_CMD} snap ls ${POOL}/${CSI_SNAP_NAME} --all"
                    CSI_SNAP_SNAP=$(eval "${RBD_CMD} snap ls ${POOL}/${CSI_SNAP_NAME} --all" 2>/dev/null | tail -n +2 | head -1)
                    if [[ -n "${CSI_SNAP_SNAP}" ]]; then
                        CS_ID=$(echo "${CSI_SNAP_SNAP}" | awk '{print $1}')
                        CS_NAME=$(echo "${CSI_SNAP_SNAP}" | awk '{print $2}')
                        echo "[INFO] Snapshot on ${CSI_SNAP_NAME}: ${CS_NAME} (ID: ${CS_ID})"
                        echo ""
                        
                        CSI_SNAP_JSON=$(echo "${CSI_SNAP_JSON}" | jq \
                            --arg sname "${CS_NAME}" --arg sid "${CS_ID}" \
                            '.snapshot = {name: $sname, id: $sid}')
                        
                        echo "[DEBUG] Executing: ${RBD_CMD} children ${POOL}/${CSI_SNAP_NAME} --snap-id ${CS_ID} --all"
                        CLONE_OUT=$(eval "${RBD_CMD} children ${POOL}/${CSI_SNAP_NAME} --snap-id ${CS_ID} --all" 2>/dev/null)
                        if [[ -n "${CLONE_OUT}" ]]; then
                            echo "[INFO] Found clones for ${CSI_SNAP_NAME}:"
                            while IFS= read -r CLONE_LINE; do
                                [[ -z "${CLONE_LINE}" ]] && continue
                                CLONE_NAME=$(echo "${CLONE_LINE}" | awk '{print $1}' | sed 's|.*/||')
                                PV_CHECK=$(echo "${PV_DATA}" | grep "${CLONE_NAME}")
                                if [[ -n "${PV_CHECK}" ]]; then
                                    HAS_PV=true
                                    echo "[INFO]   - ${CLONE_NAME} [HAS PV]"
                                else
                                    HAS_PV=false
                                    echo "[INFO]   - ${CLONE_NAME} [NO PV]"
                                fi
                                CSI_SNAP_JSON=$(echo "${CSI_SNAP_JSON}" | jq \
                                    --arg cname "${CLONE_NAME}" --argjson hpv "${HAS_PV}" \
                                    '.clones += [{name: $cname, has_pv: $hpv}]')
                            done <<< "${CLONE_OUT}"
                            echo ""
                        fi
                    fi
                    CHAIN_JSON=$(echo "${CHAIN_JSON}" | jq --argjson cs "${CSI_SNAP_JSON}" '.csi_snaps += [$cs]')
                else
                    echo "[DEBUG] ${TRASH_NAME} is not a child of ${IMAGE_NAME}"
                fi
            done <<< "${TRASH_SNAPS}"
        else
            echo "[DEBUG] No csi-snap images found in trash"
            echo ""
        fi
        
        echo "[DEBUG] Finished processing snapshot ${S_NAME}"
        echo ""
    done <<< "${PARENT_SNAPS}"
    
    echo "[DEBUG] Finished processing all snapshots for parent ${IMAGE_NAME}"
    echo ""

    # Fallback if no csi-snap found via rbd children
    CSI_SNAP_COUNT=$(echo "${CHAIN_JSON}" | jq '.csi_snaps | length')
    if [[ "${CSI_SNAP_COUNT}" -eq 0 ]]; then
        echo "----------------------------------------------"
        echo "[INFO] No children found via snapshots. Searching pool directly..."
        echo ""
        echo "[DEBUG] Executing: ${RBD_CMD} ls ${POOL}"
        for CANDIDATE in $(eval "${RBD_CMD} ls ${POOL}" 2>/dev/null | grep "csi-snap"); do
            echo "[DEBUG] Checking candidate: ${CANDIDATE}"
            PARENT_CHECK=$(eval "${RBD_CMD} info ${POOL}/${CANDIDATE}" 2>/dev/null | grep "parent:")
            if echo "${PARENT_CHECK}" | grep -q "${IMAGE_ID}\|${IMAGE_NAME}"; then
                echo "[INFO] Found child via parent match: ${CANDIDATE}"
                echo ""

                # Check VSC using UUID matching
                PARENT_UUID=$(echo "${IMAGE_NAME}" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
                VSC_CHECK=""
                if [[ -n "${PARENT_UUID}" ]]; then
                    VSC_CHECK=$(echo "${VSC_DATA}" | grep "volumeHandle:" | grep "${PARENT_UUID}")
                fi
                
                if [[ -n "${VSC_CHECK}" ]]; then
                    echo "[INFO] VSC found for parent ${IMAGE_NAME}. Skipping ${CANDIDATE}."
                    echo ""
                    continue
                fi

                CSI_SNAP_JSON=$(jq -n --arg name "${CANDIDATE}" '{name: $name, snapshot: null, clones: []}')

                echo "[DEBUG] Executing: ${RBD_CMD} snap ls ${POOL}/${CANDIDATE} --all"
                CSI_SNAP_SNAP=$(eval "${RBD_CMD} snap ls ${POOL}/${CANDIDATE} --all" 2>/dev/null | tail -n +2 | head -1)
                if [[ -n "${CSI_SNAP_SNAP}" ]]; then
                    CS_ID=$(echo "${CSI_SNAP_SNAP}" | awk '{print $1}')
                    CS_NAME=$(echo "${CSI_SNAP_SNAP}" | awk '{print $2}')
                    echo "[INFO] Snapshot on ${CANDIDATE}: ${CS_NAME} (ID: ${CS_ID})"
                    echo ""
                    
                    CSI_SNAP_JSON=$(echo "${CSI_SNAP_JSON}" | jq \
                        --arg sname "${CS_NAME}" --arg sid "${CS_ID}" \
                        '.snapshot = {name: $sname, id: $sid}')

                    echo "[DEBUG] Executing: ${RBD_CMD} children ${POOL}/${CANDIDATE} --snap-id ${CS_ID} --all"
                    CLONE_OUT=$(eval "${RBD_CMD} children ${POOL}/${CANDIDATE} --snap-id ${CS_ID} --all" 2>/dev/null)
                    if [[ -n "${CLONE_OUT}" ]]; then
                        echo "[INFO] Found clones for ${CANDIDATE}:"
                        while IFS= read -r CLONE_LINE; do
                            [[ -z "${CLONE_LINE}" ]] && continue
                            CLONE_NAME=$(echo "${CLONE_LINE}" | awk '{print $1}' | sed 's|.*/||')
                            PV_CHECK=$(echo "${PV_DATA}" | grep "${CLONE_NAME}")
                            if [[ -n "${PV_CHECK}" ]]; then
                                HAS_PV=true
                                echo "[INFO]   - ${CLONE_NAME} [HAS PV]"
                            else
                                HAS_PV=false
                                echo "[INFO]   - ${CLONE_NAME} [NO PV]"
                            fi
                            CSI_SNAP_JSON=$(echo "${CSI_SNAP_JSON}" | jq \
                                --arg cname "${CLONE_NAME}" --argjson hpv "${HAS_PV}" \
                                '.clones += [{name: $cname, has_pv: $hpv}]')
                        done <<< "${CLONE_OUT}"
                        echo ""
                    fi
                fi
                CHAIN_JSON=$(echo "${CHAIN_JSON}" | jq --argjson cs "${CSI_SNAP_JSON}" '.csi_snaps += [$cs]')
            fi
        done
    fi

    # Only save chain if it has csi-snaps to clean
    CSI_SNAP_COUNT=$(echo "${CHAIN_JSON}" | jq '.csi_snaps | length')
    if [[ "${CSI_SNAP_COUNT}" -gt 0 ]]; then
        echo "[INFO] Saving chain with ${CSI_SNAP_COUNT} csi-snap(s) to ${OUTPUT_FILE}"
        jq --argjson chain "${CHAIN_JSON}" '. += [$chain]' ${OUTPUT_FILE} > ${OUTPUT_FILE}.tmp && mv ${OUTPUT_FILE}.tmp ${OUTPUT_FILE}
    else
        echo "[INFO] No csi-snaps to clean for ${IMAGE_NAME}. Skipping."
    fi

    echo ""
    echo "=============================================="
    echo ""
done

echo "=============================================="
echo "DISCOVERY COMPLETE"
echo "=============================================="
echo "[INFO] Data written to ${OUTPUT_FILE}"
echo ""
echo "Next steps:"
echo "  - View dependency tree:  bash 03_view_tree.sh"
echo "  - Start cleanup:         bash 02_cleanup.sh"
echo ""
