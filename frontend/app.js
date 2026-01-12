/**
 * DataBridge ERP - Frontend Application
 * Handles UI interactions, API calls, and state management
 */

// Configuration - Update this after CDK deployment
const CONFIG = {
    API_BASE_URL: 'https://61t3yw5zp4.execute-api.ap-south-1.amazonaws.com/v1',
    POLL_INTERVAL: 3000, // Job status polling interval (ms)
};

// State
const state = {
    currentSection: 'ingest',
    selectedFile: null,
    jobs: [],
    pollingTimer: null,
};

// ============================================
// Utility Functions
// ============================================

/**
 * Show a toast notification
 */
function showToast(type, title, message) {
    const container = document.getElementById('toastContainer');

    const icons = {
        success: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>',
        error: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
        info: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>',
    };

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `
        <div class="toast-icon">${icons[type]}</div>
        <div class="toast-content">
            <div class="toast-title">${title}</div>
            <div class="toast-message">${message}</div>
        </div>
    `;

    container.appendChild(toast);

    // Auto-remove after 5 seconds
    setTimeout(() => {
        toast.style.animation = 'slideIn 0.3s ease reverse';
        setTimeout(() => toast.remove(), 300);
    }, 5000);
}

/**
 * Show/hide loading overlay
 */
function setLoading(loading) {
    const overlay = document.getElementById('loadingOverlay');
    overlay.classList.toggle('hidden', !loading);
}

/**
 * Format file size
 */
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

/**
 * Format date
 */
function formatDate(isoString) {
    const date = new Date(isoString);
    return date.toLocaleString();
}

// ============================================
// API Functions
// ============================================

/**
 * Make API request
 */
async function apiRequest(endpoint, method = 'GET', body = null) {
    if (!CONFIG.API_BASE_URL) {
        showToast('error', 'Configuration Error', 'API URL not configured. Please deploy the CDK stack first.');
        throw new Error('API URL not configured');
    }

    const options = {
        method,
        headers: {
            'Content-Type': 'application/json',
        },
    };

    if (body) {
        options.body = JSON.stringify(body);
    }

    const response = await fetch(`${CONFIG.API_BASE_URL}${endpoint}`, options);
    const data = await response.json();

    if (!response.ok) {
        throw new Error(data.error || 'API request failed');
    }

    return data;
}

/**
 * Trigger ingestion
 */
async function triggerIngestion(sourceType, tableName, config) {
    return apiRequest('/ingest', 'POST', {
        source_type: sourceType,
        table_name: tableName,
        config,
    });
}

/**
 * Upload file
 */
async function uploadFile(file, tableName) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();

        reader.onload = async () => {
            try {
                const base64Content = reader.result.split(',')[1];

                const response = await apiRequest('/upload', 'POST', {
                    filename: file.name,
                    table_name: tableName,
                    content: base64Content,
                });

                resolve(response);
            } catch (error) {
                reject(error);
            }
        };

        reader.onerror = () => reject(new Error('Failed to read file'));
        reader.readAsDataURL(file);
    });
}

/**
 * Fetch job status
 */
async function fetchJobs(statusFilter = null) {
    let endpoint = '/status';
    if (statusFilter) {
        endpoint += `?status=${statusFilter}`;
    }
    return apiRequest(endpoint);
}

/**
 * Fetch single job
 */
async function fetchJob(jobId) {
    return apiRequest(`/status/${jobId}`);
}

// ============================================
// UI Functions
// ============================================

/**
 * Switch active section
 */
function switchSection(sectionName) {
    // Update nav items
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.toggle('active', item.dataset.section === sectionName);
    });

    // Update sections
    document.querySelectorAll('.content-section').forEach(section => {
        section.classList.toggle('active', section.id === `${sectionName}Section`);
    });

    // Update page title
    const titles = {
        ingest: 'Ingest Data',
        upload: 'Upload File',
        status: 'Job Status',
    };
    document.getElementById('pageTitle').textContent = titles[sectionName];

    state.currentSection = sectionName;

    // Refresh jobs when switching to status section
    if (sectionName === 'status') {
        refreshJobs();
    }
}

/**
 * Show source-specific configuration
 */
function showSourceConfig(sourceType) {
    // Hide all configs
    document.querySelectorAll('.source-config').forEach(config => {
        config.classList.add('hidden');
    });

    // Show selected config
    if (sourceType) {
        const configId = sourceType === 'api' ? 'apiConfig' : `${sourceType}Config`;
        const config = document.getElementById(configId);
        if (config) {
            config.classList.remove('hidden');
        }
    }
}

/**
 * Render jobs list
 */
function renderJobs(jobs) {
    const container = document.getElementById('jobsList');

    if (!jobs || jobs.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="12" cy="12" r="10"/>
                    <line x1="12" y1="8" x2="12" y2="12"/>
                    <line x1="12" y1="16" x2="12.01" y2="16"/>
                </svg>
                <p>No jobs found</p>
            </div>
        `;
        return;
    }

    container.innerHTML = jobs.map(job => `
        <div class="job-item">
            <div class="job-header">
                <div>
                    <div class="job-title">${job.table_name || job.filename || 'Unknown'}</div>
                    <div class="job-id">${job.job_id}</div>
                </div>
                <span class="job-status ${job.status.toLowerCase()}">${job.status}</span>
            </div>
            <div class="job-progress">
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${job.progress}%"></div>
                </div>
            </div>
            <div class="job-message">${job.message || ''}</div>
            <div class="job-meta">
                <span>Source: ${job.source_type || 'upload'}</span>
                <span>Created: ${formatDate(job.created_at)}</span>
                ${job.row_count ? `<span>Rows: ${job.row_count}</span>` : ''}
            </div>
        </div>
    `).join('');
}

/**
 * Refresh jobs list
 */
async function refreshJobs() {
    try {
        const filter = document.getElementById('statusFilter').value;
        const response = await fetchJobs(filter || null);
        state.jobs = response.jobs || [];
        renderJobs(state.jobs);
    } catch (error) {
        console.error('Failed to fetch jobs:', error);
        // Don't show error toast for polling
    }
}

/**
 * Handle file selection
 */
function handleFileSelect(file) {
    if (!file) return;

    // Validate file type
    const validExtensions = ['csv', 'json', 'xls', 'xlsx', 'txt'];
    const ext = file.name.split('.').pop().toLowerCase();

    if (!validExtensions.includes(ext)) {
        showToast('error', 'Invalid File', `Unsupported file format: .${ext}`);
        return;
    }

    // Validate file size (10MB max)
    const maxSize = 10 * 1024 * 1024;
    if (file.size > maxSize) {
        showToast('error', 'File Too Large', 'Maximum file size is 10MB');
        return;
    }

    state.selectedFile = file;

    // Update UI
    document.getElementById('uploadArea').classList.add('hidden');
    document.getElementById('selectedFile').classList.remove('hidden');
    document.getElementById('fileName').textContent = file.name;
    document.getElementById('fileSize').textContent = formatFileSize(file.size);
    document.getElementById('uploadBtn').disabled = false;
}

/**
 * Remove selected file
 */
function removeSelectedFile() {
    state.selectedFile = null;
    document.getElementById('uploadArea').classList.remove('hidden');
    document.getElementById('selectedFile').classList.add('hidden');
    document.getElementById('fileInput').value = '';
    document.getElementById('uploadBtn').disabled = true;
}

/**
 * Get ingestion config from form
 */
function getIngestionConfig(sourceType) {
    switch (sourceType) {
        case 'ftp':
            return {
                host: document.getElementById('ftpHost').value,
                port: parseInt(document.getElementById('ftpPort').value) || 21,
                username: document.getElementById('ftpUsername').value,
                password: document.getElementById('ftpPassword').value,
                file_path: document.getElementById('ftpFilePath').value,
            };

        case 'http':
            let headers = {};
            try {
                const headersStr = document.getElementById('httpHeaders').value;
                if (headersStr) headers = JSON.parse(headersStr);
            } catch (e) {
                showToast('error', 'Invalid JSON', 'Headers must be valid JSON');
                throw e;
            }

            return {
                url: document.getElementById('httpUrl').value,
                method: document.getElementById('httpMethod').value,
                filename: document.getElementById('httpFilename').value,
                headers,
            };

        case 'tcp':
            return {
                host: document.getElementById('tcpHost').value,
                port: parseInt(document.getElementById('tcpPort').value),
                filename: document.getElementById('tcpFilename').value,
            };

        case 'api':
            return {
                url: document.getElementById('apiUrl').value,
                auth: {
                    type: document.getElementById('apiAuthType').value,
                    token: document.getElementById('apiToken').value,
                },
            };

        default:
            return {};
    }
}

// ============================================
// Event Handlers
// ============================================

function initEventHandlers() {
    // Navigation
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            switchSection(item.dataset.section);
        });
    });

    // Source type selection
    document.getElementById('sourceType').addEventListener('change', (e) => {
        showSourceConfig(e.target.value);
    });

    // Ingest form submission
    document.getElementById('ingestForm').addEventListener('submit', async (e) => {
        e.preventDefault();

        const sourceType = document.getElementById('sourceType').value;
        const tableName = document.getElementById('tableName').value;

        if (!sourceType || !tableName) {
            showToast('error', 'Missing Fields', 'Please fill in all required fields');
            return;
        }

        try {
            setLoading(true);
            const config = getIngestionConfig(sourceType);
            const response = await triggerIngestion(sourceType, tableName, config);

            showToast('success', 'Ingestion Started', `Job ID: ${response.job_id}`);

            // Switch to status section
            switchSection('status');
        } catch (error) {
            showToast('error', 'Ingestion Failed', error.message);
        } finally {
            setLoading(false);
        }
    });

    // Upload area interactions
    const uploadArea = document.getElementById('uploadArea');
    const fileInput = document.getElementById('fileInput');

    uploadArea.addEventListener('click', () => fileInput.click());

    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.classList.add('drag-over');
    });

    uploadArea.addEventListener('dragleave', () => {
        uploadArea.classList.remove('drag-over');
    });

    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.classList.remove('drag-over');

        const file = e.dataTransfer.files[0];
        handleFileSelect(file);
    });

    fileInput.addEventListener('change', (e) => {
        handleFileSelect(e.target.files[0]);
    });

    // Remove file button
    document.getElementById('removeFile').addEventListener('click', removeSelectedFile);

    // Upload form submission
    document.getElementById('uploadForm').addEventListener('submit', async (e) => {
        e.preventDefault();

        if (!state.selectedFile) {
            showToast('error', 'No File Selected', 'Please select a file to upload');
            return;
        }

        const tableName = document.getElementById('uploadTableName').value;

        if (!tableName) {
            showToast('error', 'Missing Table Name', 'Please enter a table name');
            return;
        }

        try {
            setLoading(true);
            const response = await uploadFile(state.selectedFile, tableName);

            showToast('success', 'Upload Successful', `Job ID: ${response.job_id}`);

            // Reset form
            removeSelectedFile();
            document.getElementById('uploadTableName').value = '';

            // Switch to status section
            switchSection('status');
        } catch (error) {
            showToast('error', 'Upload Failed', error.message);
        } finally {
            setLoading(false);
        }
    });

    // Status filter
    document.getElementById('statusFilter').addEventListener('change', refreshJobs);

    // Refresh button
    document.getElementById('refreshBtn').addEventListener('click', () => {
        if (state.currentSection === 'status') {
            refreshJobs();
        }
    });
}

// ============================================
// Polling
// ============================================

function startPolling() {
    // Poll for job updates every few seconds
    state.pollingTimer = setInterval(() => {
        if (state.currentSection === 'status') {
            refreshJobs();
        }
    }, CONFIG.POLL_INTERVAL);
}

function stopPolling() {
    if (state.pollingTimer) {
        clearInterval(state.pollingTimer);
        state.pollingTimer = null;
    }
}

// ============================================
// Initialization
// ============================================

document.addEventListener('DOMContentLoaded', () => {
    initEventHandlers();
    startPolling();

    // Check if API is configured
    if (!CONFIG.API_BASE_URL) {
        showToast('info', 'API Not Configured', 'Deploy the CDK stack and update API_BASE_URL in app.js');
    }
});

// Cleanup on page unload
window.addEventListener('beforeunload', stopPolling);
