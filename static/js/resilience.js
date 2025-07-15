// ========== RESILIENCE.JS COMPLET CORRIGÉ ==========
document.addEventListener('DOMContentLoaded', function () {
    const map = L.map('resilience-map').setView([48.86, 2.35], 10);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);

    let layerStore = {}; // Stocke les couches actives

    const fileInput = document.getElementById('resilience-files');
    const fileNamesContainer = document.getElementById('file-names-container');
    const uploadBtn = document.getElementById('resilience-upload-btn');

    const layerSelect = document.getElementById('layer-selector');
    const colorPicker = document.getElementById('color-picker');
    const tableContainer = document.getElementById('table-container');
    const tableSelector = document.getElementById('table-selector');
    const layerControls = document.getElementById('layer-controls');

    // ========== Upload de fichiers ========== //
    fileInput.addEventListener('change', () => {
        fileNamesContainer.innerHTML = "";
        Array.from(fileInput.files).forEach((file, index) => {
            const div = document.createElement('div');
            div.innerHTML = `
                <label for="name-${index}">Nom pour <strong>${file.name}</strong> :</label>
                <input type="text" name="name-${index}" data-index="${index}" placeholder="ex: inondation" required>
            `;
            fileNamesContainer.appendChild(div);
        });
    });

    uploadBtn.addEventListener('click', () => {
        const files = fileInput.files;
        if (!files.length) return alert("Veuillez sélectionner des fichiers GPKG.");

        const formData = new FormData();
        Array.from(files).forEach((file, index) => {
            const nameInput = document.querySelector(`input[name="name-${index}"]`);
            if (!nameInput || !nameInput.value.trim()) {
                alert(`Veuillez donner un nom au fichier ${file.name}`);
                return;
            }
            formData.append('files', file);
            formData.append(`name-${index}`, nameInput.value.trim());
        });

        fetch('/upload_resilience', { method: 'POST', body: formData })
            .then(r => r.json())
            .then(data => {
                if (data.status === 'ok') {
                    alert("Importation réussie !");
                    updateLayerList();
                } else {
                    alert("Erreur serveur : " + data.message);
                }
            })
            .catch(err => alert("Erreur réseau : " + err.message));
    });

    // ========== Mise à jour des couches disponibles ========== //
    function updateLayerList() {
        fetch('/resilience_layers')
            .then(r => r.json())
            .then(layers => {
                // Réinitialiser tous les select et couches
                layerSelect.innerHTML = '';
                tableSelector.innerHTML = '';
                layerControls.innerHTML = '';
                layerStore = {};

                const layerA = document.getElementById('layer-a');
                const layerB = document.getElementById('layer-b');
                layerA.innerHTML = '';
                layerB.innerHTML = '';

                layers.forEach(layer => {
                    // Remplir les 4 dropdowns
                    [layerSelect, tableSelector, layerA, layerB].forEach(select => {
                        const opt = document.createElement('option');
                        opt.value = layer;
                        opt.textContent = layer;
                        select.appendChild(opt);
                    });

                    // Ajout dans la gestion des couches (check, couleur, delete)
                    const wrapper = document.createElement('div');
                    wrapper.style.marginBottom = '10px';
                    wrapper.innerHTML = `
                        <input type="checkbox" id="toggle-${layer}" class="layer-toggle" data-layer="${layer}">
                        <label for="toggle-${layer}"><strong>${layer}</strong></label>
                        <input type="color" class="layer-color" data-layer="${layer}" value="#005aa3" style="margin-left:10px;">

                        <select class="download-format" data-layer="${layer}" style="margin-left:10px;">
                            <option value="">⬇ Format</option>
                            <option value="csv">CSV</option>
                            <option value="html">HTML</option>
                        </select>

                        <button class="delete-layer-btn" data-layer="${layer}" style="margin-left:10px;">🗑 Supprimer</button>
                    `;



                    layerControls.appendChild(wrapper);
                });
            })
            .catch(err => {
                console.error("Erreur lors du chargement des couches :", err);
                alert("Impossible de charger les couches de la base.");
            });
    }

    const createBtn = document.getElementById('create-view-btn');

    createBtn.addEventListener('click', () => {
        const tableA = document.getElementById('layer-a').value;
        const tableB = document.getElementById('layer-b').value;
        const viewName = document.getElementById('view-name').value.trim();

        if (!tableA || !tableB || !viewName) {
            return alert("Veuillez remplir tous les champs.");
        }

        fetch('/create_resilience_view', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ table_a: tableA, table_b: tableB, view_name: viewName })
        })
        .then(r => r.json())
        .then(res => {
            if (res.status === 'ok') {
                alert("✅ Vue créée avec succès.");
                updateLayerList(); // <== ici c'est visible car dans le même bloc
            } else {
                alert("❌ Erreur : " + res.message);
            }
        })
        .catch(e => alert("Erreur réseau : " + e.message));
    });



    function displayLayer(layerName, color = "#005aa3") {
        // Supprime la couche si elle est déjà affichée
        if (layerStore[layerName]) {
            map.removeLayer(layerStore[layerName]);
            delete layerStore[layerName];
        }

        // Recharge la couche depuis le backend
        fetch(`/resilience_layer_data/${layerName}`)
            .then(r => r.json())
            .then(data => {
                if (data.status !== 'ok') throw new Error(data.message);

                const geo = L.geoJSON(data.features, {
                    style: { color },
                    pointToLayer: function (feature, latlng) {
                        return L.circleMarker(latlng, {
                            radius: 6,
                            color: color,
                            fillColor: color,
                            fillOpacity: 0.6
                        });
                    },
                    onEachFeature: (feature, layer) => {
                        const props = feature.properties || {};
                        const content = Object.entries(props)
                            .map(([k, v]) => `<strong>${k}</strong>: ${v}`)
                            .join('<br>');
                        layer.bindPopup(content);
                    }
                }).addTo(map);

                layerStore[layerName] = geo;
                map.fitBounds(geo.getBounds());
            })
            .catch(e => alert("Erreur : " + e.message));
    }


    function loadAttributeTable(layerName) {
        fetch(`/resilience_layer_data/${layerName}`)
            .then(r => r.json())
            .then(data => {
                if (data.status !== 'ok') throw new Error(data.message);

                const columns = data.columns.filter(c => c !== 'geometry');
                const rows = data.table;

                let html = `<table><thead><tr>${columns.map(c => `<th>${c}</th>`).join('')}</tr></thead><tbody>`;
                rows.forEach(row => {
                    html += `<tr>${columns.map(c => `<td>${row[c] ?? ''}</td>`).join('')}</tr>`;
                });
                html += '</tbody></table>';
                tableContainer.innerHTML = html;
            })
            .catch(e => {
                tableContainer.innerHTML = `<p style="color: red;">Erreur table : ${e.message}</p>`;
            });
    }

    tableSelector.addEventListener('change', () => {
        const selected = tableSelector.value;
        if (selected) loadAttributeTable(selected);
    });

    layerSelect.addEventListener('change', () => {
        displayLayer(layerSelect.value, colorPicker.value);
    });

    colorPicker.addEventListener('input', () => {
        if (layerSelect.value) displayLayer(layerSelect.value, colorPicker.value);
    });

    layerControls.addEventListener('change', function (e) {
        const layerName = e.target.dataset.layer;
        if (e.target.classList.contains('layer-toggle')) {
            const colorInput = document.querySelector(`input.layer-color[data-layer="${layerName}"]`);
            if (e.target.checked) displayLayer(layerName, colorInput.value);
            else if (layerStore[layerName]) map.removeLayer(layerStore[layerName]);
        }
        if (e.target.classList.contains('layer-color')) {
            const color = e.target.value;
            const checkbox = document.querySelector(`input.layer-toggle[data-layer="${layerName}"]`);
            if (checkbox.checked) {
                // Si visible → changer immédiatement la couleur
                displayLayer(layerName, color);
            } else {
                // Sinon → stocke la couleur, rien à faire
            }
        }
        if (e.target.classList.contains('download-format')) {
            const layer = e.target.dataset.layer;
            const format = e.target.value;
            if (format) {
                window.open(`/download_resilience_layer/${layer}?format=${format}`, '_blank');
                e.target.value = '';
            }
        }

    
    });

    layerControls.addEventListener('click', function (e) {
        if (!e.target.classList.contains('delete-layer-btn')) return;

        const layer = e.target.dataset.layer;
        fetch(`/resilience_dependencies/${layer}`)
            .then(r => r.json())
            .then(data => {
                const deps = data.dependencies;
                let msg = `Voulez-vous vraiment supprimer la couche "${layer}" ?`;
                if (deps.length) {
                    msg += `\n\n⚠️ Utilisée dans :\n - ${deps.join("\n - ")}`;
                }
                msg += "\n\nCette action est irréversible.";

                if (confirm(msg)) {
                    fetch('/delete_resilience_layer', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ layer })
                    })
                    .then(r => r.json())
                    .then(result => {
                        if (result.status === 'ok') {
                            alert(`✅ Couche "${layer}" supprimée.`);
                            updateLayerList();
                            if (layerStore[layer]) {
                                map.removeLayer(layerStore[layer]);
                                delete layerStore[layer];
                            }
                        } else {
                            alert(`❌ Erreur : ${result.message}`);
                        }
                    });
                }
                if (e.target.classList.contains('download-layer-btn')) {
                    const layer = e.target.dataset.layer;
                    window.open(`/download_resilience_layer/${layer}`, '_blank');
                }

            });
    });

    updateLayerList(); // démarrage
});
