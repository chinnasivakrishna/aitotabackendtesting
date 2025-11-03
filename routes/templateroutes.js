const express = require('express')
const router = express.Router()

const Template = require('../models/Template')
const Agent = require('../models/Agent')
const mongoose = require('mongoose')
// fetch will be imported dynamically where needed

// Create a template
router.post('/', async (req, res) => {
  try {
    const { platform, name, url, imageUrl, description } = req.body || {}
    if (!platform || !name || !url) {
      return res.status(400).json({ success: false, message: 'platform, name and url are required' })
    }
    const tpl = await Template.create({ platform, name, url, imageUrl, description, createdBy: req.user?.id })
    res.status(201).json({ success: true, data: tpl })
  } catch (e) {
    res.status(500).json({ success: false, message: e.message })
  }
})

// List templates (optionally filter by platform)
router.get('/', async (req, res) => {
  try {
    const { platform } = req.query
    const filter = platform ? { platform } : {}
    const data = await Template.find(filter).sort({ createdAt: -1 })
    res.json({ success: true, data })
  } catch (e) {
    res.status(500).json({ success: false, message: e.message })
  }
})

// Assign templates to an agent
router.post('/assign', async (req, res) => {
  try {
    const { agentId, templateIds, templates } = req.body || {}
    if (!agentId || !Array.isArray(templateIds)) {
      return res.status(400).json({ success: false, message: 'agentId and templateIds[] are required' })
    }

    const agent = await Agent.findById(agentId)
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' })

    const ids = templateIds
      .map(id => { try { return new mongoose.Types.ObjectId(String(id)) } catch { return null } })
      .filter(Boolean)

    // Ensure field exists
    if (!Array.isArray(agent.templates)) agent.templates = []

    // Merge unique
    const existing = new Set(agent.templates.map(v => String(v)))
    ids.forEach(id => { if (!existing.has(String(id))) agent.templates.push(id) })

    // Handle WhatsApp templates if provided
    if (Array.isArray(templates) && templates.length > 0) {
      // Ensure whatsappTemplates field exists
      if (!Array.isArray(agent.whatsappTemplates)) agent.whatsappTemplates = []
      
      // Add WhatsApp templates
      templates.forEach(template => {
        const existingTemplate = agent.whatsappTemplates.find(t => t.templateId === template._id)
        if (!existingTemplate) {
          agent.whatsappTemplates.push({
            templateId: template._id,
            templateName: template.name,
            templateUrl: template.url,
            description: template.description,
            language: template.language,
            status: template.status,
            category: template.category,
            assignedAt: new Date()
          })
        }
      })
    }

    await agent.save()

    return res.json({ success: true, data: { agentId: agent._id, templates: agent.templates, whatsappTemplates: agent.whatsappTemplates } })
  } catch (e) {
    res.status(500).json({ success: false, message: e.message })
  }
})

// Get templates assigned to an agent
router.get('/agent/:agentId', async (req, res) => {
  try {
    const { agentId } = req.params
    const agent = await Agent.findById(agentId).populate('templates')
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' })
    res.json({ success: true, data: agent.templates || [] })
  } catch (e) {
    res.status(500).json({ success: false, message: e.message })
  }
})

// Get templates assigned to a client (via agents)
router.get('/client/:clientId', async (req, res) => {
  try {
    const { clientId } = req.params
    const { platform, includeAgents } = req.query
    
    // Find all agents belonging to this client
    const agents = await Agent.find({ clientId, isActive: true })
    
    if (!agents || agents.length === 0) {
      return res.json({ success: true, data: [] })
    }
    
    let templates = []
    
    // If platform is specified, get platform-specific templates
    if (platform === 'whatsapp') {
      // If includeAgents=true, return per-agent entries with agent info
      if (String(includeAgents).toLowerCase() === 'true') {
        const entries = []
        agents.forEach(agent => {
          const agentName = agent.agentName || agent.name || agent.email || ''
          if (Array.isArray(agent.whatsappTemplates)) {
            agent.whatsappTemplates.forEach(t => {
              entries.push({
                agentId: String(agent._id),
                agentName,
                templateId: t.templateId,
                name: t.templateName,
                description: t.description,
                language: t.language,
                status: t.status,
                category: t.category,
                assignedAt: t.assignedAt,
                templateUrl: t.templateUrl || ''
              })
            })
          }
        })
        return res.json({ success: true, data: entries })
      }

      // Otherwise, return unique template list across agents (legacy behavior)
      agents.forEach(agent => {
        if (agent.whatsappTemplates && Array.isArray(agent.whatsappTemplates)) {
          templates = templates.concat(agent.whatsappTemplates)
        }
      })

      const uniqueTemplates = templates.filter((template, index, self) => 
        index === self.findIndex(t => t.templateId === template.templateId)
      )
      return res.json({ success: true, data: uniqueTemplates })
    } else {
      // Get regular templates from all agents
      const agentIds = agents.map(agent => agent._id)
      const templateDocs = await Template.find({ 
        _id: { $in: agents.flatMap(agent => agent.templates || []) }
      })
      
      return res.json({ success: true, data: templateDocs })
    }
  } catch (e) {
    console.error('Error fetching client templates:', e)
    res.status(500).json({ success: false, message: e.message })
  }
})

// Assign templates to a client (via agents)
router.post('/assign-client', async (req, res) => {
  try {
    const { clientId, templateIds, platform } = req.body || {}
    if (!clientId || !Array.isArray(templateIds)) {
      return res.status(400).json({ success: false, message: 'clientId and templateIds[] are required' })
    }

    // Find all agents belonging to this client
    const agents = await Agent.find({ clientId, isActive: true })
    if (!agents || agents.length === 0) {
      return res.status(404).json({ success: false, message: 'No active agents found for this client' })
    }

    // If platform is WhatsApp, assign to all agents
    if (platform === 'whatsapp') {
      // Get template data from external service
      const { default: fetch } = await import('node-fetch')
      const templateResp = await fetch('https://whatsapp-template-module.onrender.com/api/whatsapp/get-templates')
      const templateData = await templateResp.json()
      const templates = Array.isArray(templateData?.templates) ? templateData.templates : []
      
      // Update each agent
      for (const agent of agents) {
        // Ensure whatsappTemplates field exists
        if (!Array.isArray(agent.whatsappTemplates)) agent.whatsappTemplates = []
        
        // Add WhatsApp templates
        templateIds.forEach(templateId => {
          const template = templates.find(t => t.id === templateId || t.name === templateId)
          if (template) {
            const existingTemplate = agent.whatsappTemplates.find(t => t.templateId === template.id || t.templateId === template.name)
            if (!existingTemplate) {
              const bodyComponent = (template.components || []).find(c => c.type === 'BODY')
              const buttonsComp = (template.components || []).find(c => c.type === 'BUTTONS')
              const firstUrl = buttonsComp && Array.isArray(buttonsComp.buttons) && buttonsComp.buttons[0]?.url
              
              agent.whatsappTemplates.push({
                templateId: template.id || template.name,
                templateName: template.name,
                templateUrl: firstUrl || '',
                description: bodyComponent?.text || '',
                language: template.language,
                status: 'APPROVED',
                category: template.category,
                assignedAt: new Date()
              })
            }
          }
        })
        
        await agent.save()
      }
      
      return res.json({ success: true, message: 'Templates assigned to client agents successfully' })
    } else {
      // For other platforms, assign regular templates
      const ids = templateIds
        .map(id => { try { return new mongoose.Types.ObjectId(String(id)) } catch { return null } })
        .filter(Boolean)

      // Update each agent
      for (const agent of agents) {
        // Ensure templates field exists
        if (!Array.isArray(agent.templates)) agent.templates = []
        
        // Merge unique
        const existing = new Set(agent.templates.map(v => String(v)))
        ids.forEach(id => { if (!existing.has(String(id))) agent.templates.push(id) })
        
        await agent.save()
      }
      
      return res.json({ success: true, message: 'Templates assigned to client agents successfully' })
    }
  } catch (e) {
    console.error('Error assigning templates to client:', e)
    res.status(500).json({ success: false, message: e.message })
  }
})

// Remove or restore an agent's WhatsApp template
// POST /api/v1/templates/agent/template-status
// body: { agentId, templateId, action: 'remove'|'restore' }
router.post('/agent/template-status', async (req, res) => {
  try {
    const { agentId, templateId, action } = req.body || {}
    console.log('agentId', agentId)
    console.log('templateId', templateId)
    console.log('action', action)
    if (!agentId || !templateId || !['remove', 'restore'].includes(String(action))) {
      return res.status(400).json({ success: false, message: 'agentId, templateId and action(remove|restore) are required' })
    }

    const agent = await Agent.findById(agentId)
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' })

    if (!Array.isArray(agent.whatsappTemplates)) agent.whatsappTemplates = []
    const tpl = agent.whatsappTemplates.find(t => String(t.templateId) === String(templateId))
    if (!tpl) return res.status(404).json({ success: false, message: 'Template not found on agent' })

    if (action === 'remove') {
      tpl.status = 'REMOVED'
      // Clear defaultTemplate if it points to this template
      if (agent.defaultTemplate && String(agent.defaultTemplate.templateId) === String(templateId) && agent.defaultTemplate.platform === 'whatsapp') {
        agent.defaultTemplate = undefined
      }
    } else if (action === 'restore') {
      tpl.status = 'APPROVED'
    }

    await agent.save()
    return res.json({ success: true, data: agent.whatsappTemplates })
  } catch (e) {
    console.error('Error updating agent template status:', e)
    return res.status(500).json({ success: false, message: e.message })
  }
})

module.exports = router