function runAnalysisCable() {
    console.log("Début de l'analyse des câbles...");

    // Récupérer la date d'exportation sélectionnée
    const exportDateField = document.getElementById('export-date-single');
    if (!exportDateField) {
        console.error("Le champ de date 'export-date-single' est introuvable !");
        return;
    }

    const exportDate = exportDateField.value;
    if (!exportDate) {
        alert("Veuillez sélectionner une date d'export.");
        console.log("Aucune date sélectionnée.");
        return;
    }

    console.log("Date sélectionnée :", exportDate);

    // Désactiver le bouton d'analyse et afficher un indicateur de chargement
    const analyzeButton = document.getElementById('analyze-cable');
    const resultsContainer = document.getElementById('results-single');
    if (!analyzeButton) {
        console.error("Le bouton 'analyze-cable' est introuvable !");
        return;
    }

    if (!resultsContainer) {
        console.error("Le conteneur de résultats 'results-single' est introuvable !");
        return;
    }

    analyzeButton.disabled = true;
    resultsContainer.innerHTML = "<p>Analyse en cours...</p>";

    console.log("Envoi de la requête à '/analyze_cable' avec la date :", exportDate);

    // Envoi de la requête POST au backend
    fetch('/analyze_cable', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ export_date: `${exportDate}_t_cable.csv` }) // Ajout de l'extension .csv à la table
    })
        .then(response => {
            if (!response.ok) {
                throw new Error("Erreur lors de l'analyse des câbles");
            }
            console.log("Réponse reçue du serveur.");
            return response.json();
        })
        .then(data => {
            console.log("Données reçues :", data);

            if (!data.results || data.results.length === 0) {
                resultsContainer.innerHTML = `<p style="color: red;">Aucun résultat trouvé.</p>`;
                return;
            }

            let html = "<h3>Résultats de l'Analyse des Câbles</h3>";
            html += "<table border='1'><thead><tr>";
            html += "<th>cb_capafo</th><th>Nombre Total</th><th>Somme Longueur</th><th>Propriétaire</th><th>Territoire</th><th>Hors Territoire</th><th>INDT</th>";
            html += "</tr></thead><tbody>";

            data.results.forEach(row => {
                html += `<tr>
                    <td>${row.cb_capafo || '-'}</td>
                    <td>${row.Nombre_Total || '-'}</td>
                    <td>${row.Somme_Longueur || '-'}</td>
                    <td>${row.Proprietaire || '-'}</td>
                    <td>${row.Territoire || '-'}</td>
                    <td>${row.Hors_Territoire || '-'}</td>
                    <td>${row.INDT || '-'}</td>
                </tr>`;
            });
            html += "</tbody></table>";

            html += `<div class="downloads">
                        <a href="${data.csv_path}" class="btn-download">Télécharger CSV</a>
                        <a href="${data.html_path}" class="btn-download">Télécharger HTML</a>
                    </div>`;

            resultsContainer.innerHTML = html;
        })
        .catch(error => {
            console.error("Erreur lors de l'analyse des câbles :", error);
            resultsContainer.innerHTML = `<p style="color: red;">Erreur : ${error.message}</p>`;
        })
        .finally(() => {
            analyzeButton.disabled = false;
        });
}

// Ajouter un écouteur d'événement pour le bouton
document.addEventListener('DOMContentLoaded', function () {
    const analyzeCableButton = document.getElementById('analyze-cable');
    if (analyzeCableButton) {
        console.log("Bouton 'analyze-cable' trouvé, ajout de l'écouteur d'événement.");
        analyzeCableButton.addEventListener('click', runAnalysisCable);
    } else {
        console.error("Le bouton 'analyze-cable' est introuvable dans le DOM.");
    }
});
